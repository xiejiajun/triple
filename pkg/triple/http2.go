/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package triple

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"github.com/dubbogo/triple/internal/buffer"
	"github.com/dubbogo/triple/internal/stream"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	h2 "github.com/dubbogo/net/http2"
	h2Triple "github.com/dubbogo/net/http2/triple"
	"github.com/golang/protobuf/proto"
	perrors "github.com/pkg/errors"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

// load codec support impl, can be import every where
import (
	"github.com/dubbogo/triple/internal/codec"
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/pkg/common"
)

// H2Controller is used by dubbo3 client/server, to call http2
type H2Controller struct {
	// client stores http2 client
	client http.Client

	// address stores target ip:port
	address string

	// mdMap strMap is used to store discover of user impl function
	mdMap  map[string]grpc.MethodDesc
	strMap map[string]grpc.StreamDesc

	// url is to get protocol, which is key of triple components, like codec header
	// url is also used to init triple header
	url *dubboCommon.URL

	// pkgHandler is to convert between raw data and frame data
	pkgHandler common.PackageHandler

	// service is user impl service
	service common.Dubbo3GrpcService

	closeChan chan struct{}

	// defaultReadBufferSize is 10M by default
	defaultReadBufferSize int
}

// skipHeader is to skip first 5 byte from dataframe with header
func skipHeader(frameData []byte) ([]byte, uint32) {
	if len(frameData) < 5 {
		return []byte{}, 0
	}
	lineHeader := frameData[:5]
	length := binary.BigEndian.Uint32(lineHeader[1:])
	return frameData[5:], length
}

// readSplitedDatas is called when client want to receive data from server
// the param @rBody is from http response. readSplitedDatas can read from it. As data from reader is not a block of data,
// but split data stream, so there needs unpacking and merging logic with split data that receive.
func (hc *H2Controller) readSplitedDatas(rBody io.ReadCloser) chan buffer.BufferMsg {
	cbm := make(chan buffer.BufferMsg)
	go func() {
		buf := make([]byte, hc.defaultReadBufferSize)
		for {
			splitBuffer := buffer.BufferMsg{
				Buffer: bytes.NewBuffer(make([]byte, 0)),
			}

			// fromFrameHeaderDataSize is wanting data size now
			fromFrameHeaderDataSize := uint32(0)
			for {
				var n int
				var err error
				if splitBuffer.Len() < int(fromFrameHeaderDataSize) || splitBuffer.Len() == 0 {
					n, err = rBody.Read(buf)
				}

				if err != nil {
					cbm <- buffer.BufferMsg{
						MsgType: buffer.ServerStreamCloseMsgType,
					}
					return
				}
				splitedData := buf[:n]
				splitBuffer.Write(splitedData)
				if fromFrameHeaderDataSize == 0 {
					// should parse data frame header first
					data := splitBuffer.Bytes()
					var totalSize uint32
					if data, totalSize = skipHeader(data); totalSize == 0 {
						break
					} else {
						// get wanting data size from header
						fromFrameHeaderDataSize = totalSize
					}
					splitBuffer.Reset()
					splitBuffer.Write(data)
				}
				if splitBuffer.Len() >= int(fromFrameHeaderDataSize) {
					allDataBody := make([]byte, fromFrameHeaderDataSize)
					splitBuffer.Read(allDataBody)
					cbm <- buffer.BufferMsg{
						Buffer:  bytes.NewBuffer(allDataBody),
						MsgType: buffer.DataMsgType,
					}
					// temp data is sent, and reset wanting data size
					fromFrameHeaderDataSize = 0
				}
			}
		}
	}()
	return cbm
}

// GetHandler is called by server when receiving tcp conn, to deal with http2 request
func (hc *H2Controller) GetHandler() func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		// triple trailer fields
		var (
			grpcMessage   = ""
			grpcCode      = 0
			traceProtoBin = 0
		)
		// load handler and header
		headerHandler, _ := common.GetProtocolHeaderHandler(hc.url.Protocol, nil, nil)
		header := headerHandler.ReadFromTripleReqHeader(r)

		// new server stream
		stream := hc.newServerStreamFromTripleHedaer(header)
		if stream == nil {
			logger.Error("creat server stream error!")
		}
		sendChan := stream.GetSend()
		closeChan := make(chan struct{})

		// start receiving from http2 server, and forward to upper proxy invoker
		ch := hc.readSplitedDatas(r.Body)
		go func() {
			for {
				select {
				case <-closeChan:
					stream.Close()
					return
				case msgData := <-ch:
					if msgData.MsgType == buffer.ServerStreamCloseMsgType {
						return
					}
					data := hc.pkgHandler.Pkg2FrameData(msgData.Bytes())
					// send to upper proxy invoker to exec
					stream.PutRecv(data, buffer.DataMsgType)
				}
			}
		}()

		// first response header
		w.Header().Add("content-type", "application/grpc+proto")

		// start receiving response from upper proxy invoker, and forward to remote http2 client
	LOOP:
		for {
			select {
			case <-hc.closeChan:
				grpcCode = int(codes.Canceled)
				grpcMessage = "triple server canceled by force" // encodeGrpcMessage(sendMsg.st.Message())
				// call finished by force
				break LOOP
			case sendMsg := <-sendChan:
				if sendMsg.Buffer == nil || sendMsg.MsgType != buffer.DataMsgType {
					if sendMsg.Status != nil {
						grpcCode = int(sendMsg.Status.Code())
						grpcMessage = "message error" // encodeGrpcMessage(sendMsg.st.Message())
					}
					// call finished
					break LOOP
				}
				sendData := sendMsg.Bytes()
				w.Write(sendData)
			}
		}

		// second response header with trailer fields
		headerHandler.WriteTripleFinalRspHeaderField(w, grpcCode, grpcMessage, traceProtoBin)

		// close all related go routines
		close(closeChan)
	}
}

// getMethodAndStreamDescMap get unary method desc map and stream method desc map from dubbo3 stub
func getMethodAndStreamDescMap(ds common.Dubbo3GrpcService) (map[string]grpc.MethodDesc, map[string]grpc.StreamDesc, error) {
	sdMap := make(map[string]grpc.MethodDesc, 8)
	strMap := make(map[string]grpc.StreamDesc, 8)
	for _, v := range ds.ServiceDesc().Methods {
		sdMap[v.MethodName] = v
	}
	for _, v := range ds.ServiceDesc().Streams {
		strMap[v.StreamName] = v
	}
	return sdMap, strMap, nil
}

// NewH2Controller can create H2Controller with conn
func NewH2Controller(isServer bool, service common.Dubbo3GrpcService, url *dubboCommon.URL) (*H2Controller, error) {
	var mdMap map[string]grpc.MethodDesc
	var strMap map[string]grpc.StreamDesc
	var err error
	if isServer {
		mdMap, strMap, err = getMethodAndStreamDescMap(service)
		if err != nil {
			logger.Error("new H2 controller error:", err)
			return nil, err
		}
	}

	var pkgHandler common.PackageHandler

	if url != nil {
		pkgHandler, _ = common.GetPackagerHandler(url.Protocol)
	}
	defaultMaxCurrentStream := atomic.Uint32{}
	defaultMaxCurrentStream.Store(math.MaxUint32)

	client := http.Client{
		Transport: &h2.Transport{
			// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
	h2c := &H2Controller{
		url:                   url,
		client:                client,
		mdMap:                 mdMap,
		strMap:                strMap,
		service:               service,
		pkgHandler:            pkgHandler,
		defaultReadBufferSize: common.DefaultHttp2ControllerReadBufferSize,
		closeChan:             make(chan struct{}),
	}
	return h2c, nil
}

// newServerStreamFromTripleHedaer can create a serverStream and add to h2Controller by @data read from frame,
// after receiving a request from client.
func (h *H2Controller) newServerStreamFromTripleHedaer(data h2Triple.ProtocolHeader) stream.Stream {
	methodName := strings.Split(data.GetPath(), "/")[2]
	md, okm := h.mdMap[methodName]
	streamd, oks := h.strMap[methodName]
	if !okm && !oks {
		logger.Errorf("method name %s not found in desc", methodName)
		return nil
	}
	var newstm *stream.ServerStream
	var err error
	if okm {
		newstm, err = stream.NewServerStream(data, md, h.url, h.service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return nil
		}
	} else {
		newstm, err = stream.NewServerStream(data, streamd, h.url, h.service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return nil
		}
	}
	return newstm
}

// StreamInvoke can start streaming invocation, called by triple client
func (h *H2Controller) StreamInvoke(ctx context.Context, method string) (grpc.ClientStream, error) {
	clientStream := stream.NewClientStream()
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("get serilizer error = ", err)
		return nil, err
	}

	tosend := clientStream.GetSend()
	sendStreamChan := make(chan h2Triple.BufferMsg)
	closeChan := make(chan struct{})
	go func() {
		for {
			select {
			case <-closeChan:
				clientStream.Close()
				return
			case sendMsg := <-tosend:
				sendStreamChan <- h2Triple.BufferMsg{
					Buffer:  bytes.NewBuffer(sendMsg.Bytes()),
					MsgType: h2Triple.MsgType(sendMsg.MsgType),
				}
			}
		}
	}()
	headerHandler, _ := common.GetProtocolHeaderHandler(h.url.Protocol, h.url, ctx)
	stremaReq := h2Triple.StreamingRequest{
		SendChan: sendStreamChan,
		Handler:  headerHandler,
	}
	go func() {
		rsp, err := h.client.Post("https://"+h.address+method, "application/grpc+proto", &stremaReq)
		if err != nil {
			panic(err)
		}
		ch := h.readSplitedDatas(rsp.Body)
	LOOP:
		for {
			select {
			case <-h.closeChan:
				close(closeChan)
			case data := <-ch:
				if data.Buffer == nil || data.MsgType == buffer.ServerStreamCloseMsgType {
					// stream receive done, close send go routine
					close(closeChan)
					break LOOP
				}
				pkg := h.pkgHandler.Pkg2FrameData(data.Bytes())
				clientStream.PutRecv(pkg, buffer.DataMsgType)
			}

		}
		trailer := rsp.Body.(*h2Triple.ResponseBody).GetTrailer()
		code, _ := strconv.Atoi(trailer.Get(codec.TrailerKeyGrpcStatus))
		msg := trailer.Get(codec.TrailerKeyGrpcMessage)
		if codes.Code(code) != codes.OK {
			logger.Errorf("grpc status not success,msg = %s, code = %d", msg, code)
		}

	}()

	pkgHandler, err := common.GetPackagerHandler(h.url.Protocol)
	return stream.NewClientUserStream(clientStream, serilizer, pkgHandler), nil
}

// UnaryInvoke can start unary invocation, called by dubbo3 client
func (h *H2Controller) UnaryInvoke(ctx context.Context, method string, data []byte, reply interface{}) error {
	sendStreamChan := make(chan h2Triple.BufferMsg, 1)
	sendStreamChan <- h2Triple.BufferMsg{
		Buffer:  bytes.NewBuffer(h.pkgHandler.Pkg2FrameData(data)),
		MsgType: h2Triple.MsgType(buffer.DataMsgType),
	}

	headerHandler, _ := common.GetProtocolHeaderHandler(h.url.Protocol, h.url, ctx)
	stremaReq := h2Triple.StreamingRequest{
		SendChan: sendStreamChan,
		Handler:  headerHandler,
	}

	rsp, err := h.client.Post("https://"+h.address+method, "application/grpc+proto", &stremaReq)
	if err != nil {
		logger.Errorf("triple unary invoke error = %v", err)
		return err
	}
	readBuf := make([]byte, h.defaultReadBufferSize)

	// splitBuffer is to temporarily store collected split data, and add them together
	splitBuffer := buffer.BufferMsg{
		Buffer: bytes.NewBuffer(make([]byte, 0)),
	}

	fromFrameHeaderDataSize := uint32(0)
	for {
		n, err := rsp.Body.Read(readBuf)
		if err != nil {
			logger.Errorf("dubbo3 unary invoke read error = %v\n", err)
			return err
		}
		splitedData := readBuf[:n]
		if fromFrameHeaderDataSize == 0 {
			// should parse data frame header first
			var totalSize uint32
			if splitedData, totalSize = h.pkgHandler.Frame2PkgData(splitedData); totalSize == 0 {
				return nil
			} else {
				fromFrameHeaderDataSize = totalSize
			}
			splitBuffer.Reset()
		}
		splitBuffer.Write(splitedData)
		if splitBuffer.Len() > int(fromFrameHeaderDataSize) {
			logger.Error("dubbo3 unary invoke error = Receive Splited Data is bigger than wanted.")
			return perrors.New("dubbo3 unary invoke error = Receive Splited Data is bigger than wanted.")
		}

		if splitBuffer.Len() == int(fromFrameHeaderDataSize) {
			break
		}
	}

	// todo start ticker to avoid trailer timeout
	trailer := rsp.Body.(*h2Triple.ResponseBody).GetTrailer()
	code, err := strconv.Atoi(trailer.Get(codec.TrailerKeyGrpcStatus))
	if err != nil {
		logger.Errorf("get trailer err = %v", err)
		return perrors.Errorf("get trailer err = %v", err)
	}
	msg := trailer.Get(codec.TrailerKeyGrpcMessage)

	if codes.Code(code) != codes.OK {
		logger.Errorf("grpc status not success, msg = %s, code = %d", msg, code)
		return perrors.Errorf("grpc status not success, msg = %s, code = %d", msg, code)
	}

	// all split data are collected and to unmarshal
	if err := proto.Unmarshal(splitBuffer.Bytes(), reply.(proto.Message)); err != nil {
		logger.Errorf("client unmarshal rsp err= %v\n", err)
		return err
	}
	return nil
}

func (h2 *H2Controller) Destroy() {
	close(h2.closeChan)
}
