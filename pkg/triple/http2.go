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
	"fmt"
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/config"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	h2 "github.com/dubbogo/net/http2"
	h2Triple "github.com/dubbogo/net/http2/triple"
	"github.com/golang/protobuf/proto"
	perrors "github.com/pkg/errors"
	"google.golang.org/grpc"
)

import (
	"github.com/dubbogo/triple/internal/codec"
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/internal/message"
	"github.com/dubbogo/triple/internal/stream"
	"github.com/dubbogo/triple/pkg/common"
)

// H2Controller is used by dubbo3 client/server, to call http2
type H2Controller struct {
	// client stores http2 client
	client http.Client

	// address stores target ip:port
	address string

	// url is to get protocol, which is key of triple components, like codec header
	// url is also used to init triple header
	url *dubboCommon.URL

	// pkgHandler is to convert between raw data and frame data
	pkgHandler common.PackageHandler

	// rpcServiceMap stores is user impl services
	rpcServiceMap *sync.Map

	closeChan chan struct{}

	// option is 10M by default
	option *config.Option
}

// skipHeader is to skip first 5 byte from dataframe with header
func skipHeader(frameData []byte) ([]byte, uint32) {
	if len(frameData) < 5 {
		return []byte{}, 0
	}
	lineHeader := frameData[:5]
	// TODO 从HTTP2.0数据包的协议头中获取当前包的数据长度
	length := binary.BigEndian.Uint32(lineHeader[1:])
	return frameData[5:], length
}

// readSplitData is called when client want to receive data from server
// the param @rBody is from http response. readSplitData can read from it. As data from reader is not a block of data,
// but split data stream, so there needs unpacking and merging logic with split data that receive.
func (hc *H2Controller) readSplitData(rBody io.ReadCloser) chan message.Message {
	cbm := make(chan message.Message)
	go func() {
		buf := make([]byte, hc.option.BufferSize)
		for {
			splitBuffer := message.Message{
				Buffer: bytes.NewBuffer(make([]byte, 0)),
			}

			// fromFrameHeaderDataSize is wanting data size now
			fromFrameHeaderDataSize := uint32(0)
			for {
				var n int
				var err error
				if splitBuffer.Len() < int(fromFrameHeaderDataSize) || splitBuffer.Len() == 0 {
					// TODO 读取TCP数据包
					n, err = rBody.Read(buf)
				}

				if err != nil {
					cbm <- message.Message{
						MsgType: message.ServerStreamCloseMsgType,
					}
					return
				}
				splitedData := buf[:n]
				// TODO 将splitedData写入splitBuffer
				splitBuffer.Write(splitedData)
				// TODO fromFrameHeaderDataSize为0表示第一次读取当前数据包, 这时候需要做两件事：
				// 	1. 从协议头里面获取当前数据包body的真实长度
				//  2. 将数据包中的协议头从splitBuffer剔除掉
				if fromFrameHeaderDataSize == 0 {
					// should parse data frame header first
					data := splitBuffer.Bytes()
					var totalSize uint32
					// TODO fromFrameHeaderDataSize为0表示第一次读取当前数据包，需要解析数据包协议头获取数据包body长度
					if data, totalSize = skipHeader(data); totalSize == 0 {
						// TODO 空包直接忽略
						break
					} else {
						// get wanting data size from header
						fromFrameHeaderDataSize = totalSize
					}
					// TODO 读取新数据包时先重置splitBuffer，这是为了把HTTP2.0数据包的协议头剔除掉,这部分不是我们需要的数据
					splitBuffer.Reset()
					splitBuffer.Write(data)
				}
				// TODO splitBuffer的长度大于等于数据包协议头解析出来的数据长度以后，表示当前数据包数据都已经到了
				if splitBuffer.Len() >= int(fromFrameHeaderDataSize) {
					allDataBody := make([]byte, fromFrameHeaderDataSize)
					// TODO 将splitBuffer中的数据读入allDataBody
					_, err := splitBuffer.Read(allDataBody)
					if err != nil {
						logger.Errorf("read SplitedDatas error = %v", err)
					}
					// TODO 将这个数据包的数据通过chan发送出去，实现异步交互
					cbm <- message.Message{
						Buffer:  bytes.NewBuffer(allDataBody),
						MsgType: message.DataMsgType,
					}
					// temp data is sent, and reset wanting data size
					// TODO 数据包处理完成，重置数据包大小跟踪变量
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
		/*
			triple trailer fields:
			http 2 trailers are headers fields sent after header response and body response.

			grpcMessage is used to show error message
			grpcCode is uint type and show grpc status code
			traceProtoBin is uint type, triple defined header.
		*/
		var (
			grpcMessage   = ""
			grpcCode      = 0
			traceProtoBin = 0
		)
		// load handler and header
		headerHandler, _ := common.GetProtocolHeaderHandler(hc.url.Protocol, nil, nil)
		header := headerHandler.ReadFromTripleReqHeader(r)

		// new server stream
		st, err := hc.newServerStreamFromTripleHedaer(header)
		if st == nil || err != nil {
			logger.Errorf("creat server stream error = %v\n", err)
			rspErrMsg := fmt.Sprintf("creat server stream error = %v\n", err)
			w.WriteHeader(400)
			if _, err := w.Write([]byte(rspErrMsg)); err != nil {
				logger.Errorf("write back rsp error message %s, error", rspErrMsg)
			}
			return
			// todo handle interface/method not found error with grpc-status
		}
		sendChan := st.GetSend()
		closeChan := make(chan struct{})

		// TODO 解析HTTP2.0协议数据包，读取我们需要的数据, 由于不是常规的http请求(GET/POST...)，不能通过request.ParseForm()等方法解析TCP
		//  数据包，所以需要我们自己解析, 标准Http请求数据包解析成具体对象的逻辑可以参考gin(github.com/gin-gonic/gin) 的binding/binding.go里面的
		//  Binding.Bind(*http.Request, interface{}) error的具体实现类
		// start receiving from http2 server, and forward to upper proxy invoker
		ch := hc.readSplitData(r.Body)
		go func() {
			for {
				select {
				case <-closeChan:
					st.Close()
					return
				case msgData := <-ch:
					if msgData.MsgType == message.ServerStreamCloseMsgType {
						return
					}
					data := hc.pkgHandler.Pkg2FrameData(msgData.Bytes())
					// send to upper proxy invoker to exec
					st.PutRecv(data, message.DataMsgType)
				}
			}
		}()

		// todo  in which condition does header response not 200?
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
				if sendMsg.Buffer == nil || sendMsg.MsgType != message.DataMsgType {
					if sendMsg.Status != nil {
						grpcCode = int(sendMsg.Status.Code())
						grpcMessage = "message error" // encodeGrpcMessage(sendMsg.st.Message())
					}
					// call finished
					break LOOP
				}
				sendData := sendMsg.Bytes()
				if _, err := w.Write(sendData); err != nil {
					logger.Errorf(" receiving response from upper proxy invoker error = %v", err)
				}
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
		// TODO 这里和gRpc的相互兼容，因为ds.ServiceDesc()和gRpc一样都是通过protocol插件生成(protoc-gen-dubbo3)
		sdMap[v.MethodName] = v
	}
	for _, v := range ds.ServiceDesc().Streams {
		strMap[v.StreamName] = v
	}
	return sdMap, strMap, nil
}

// addDefaultOption fill default options to @opt
func addDefaultOption(opt *config.Option) *config.Option {
	if opt == nil {
		opt = &config.Option{}
	}
	opt.SetEmptyFieldDefaultConfig()
	return opt
}

// NewH2Controller can create H2Controller with impl @rpcServiceMap and url
// @opt can be nil or configured by user
func NewH2Controller(isServer bool, rpcServiceMap *sync.Map, url *dubboCommon.URL, opt *config.Option) (*H2Controller, error) {
	var pkgHandler common.PackageHandler

	if url != nil {
		pkgHandler, _ = common.GetPackagerHandler(url.Protocol)
	}

	// new http client struct
	var client http.Client
	if !isServer {
		client = http.Client{
			Transport: &h2.Transport{
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		}
	}

	h2c := &H2Controller{
		url:           url,
		client:        client,
		rpcServiceMap: rpcServiceMap,
		pkgHandler:    pkgHandler,
		option:        addDefaultOption(opt),
		closeChan:     make(chan struct{}),
	}
	return h2c, nil
}

/*
newServerStreamFromTripleHedaer can create a serverStream by @data read from frame, after receiving a request from client.

firstly, it checks and gets calling params interfaceKey and methodName and use interfaceKey to find if there is existing service
secondly, it judge if it is streaming rpc or unary rpc
thirdly, new stream and return

any error occurs in the above procedures are fatal, as the invocation target can't be found.
todo how to deal with errror in this procedure gracefully is to be discussed next
*/
func (hc *H2Controller) newServerStreamFromTripleHedaer(data h2Triple.ProtocolHeader) (stream.Stream, error) {
	paramsList := strings.Split(data.GetPath(), "/")
	// todo path params len check
	if len(paramsList) < 3 {
		return nil, status.Err(codes.Unimplemented, "invalid http2 triple path:"+data.GetPath())
	}
	interfaceKey := paramsList[1]
	methodName := paramsList[2]

	serviceInterface, ok := hc.rpcServiceMap.Load(interfaceKey)
	if !ok {
		return nil, status.Err(codes.Unimplemented, "not found target service key"+interfaceKey)
	}
	service, ok := serviceInterface.(common.Dubbo3GrpcService)
	if !ok {
		return nil, status.Err(codes.Internal, "can't assert impl of interface "+interfaceKey+" to dubbo RPCService")
	}

	mdMap, strMap, err := getMethodAndStreamDescMap(service)
	if err != nil {
		logger.Error("new H2 controller error:", err)
		return nil, status.Err(codes.Unimplemented, err.Error())
	}

	md, okm := mdMap[methodName]
	streamd, oks := strMap[methodName]

	if !okm && !oks {
		logger.Errorf("method name %s not found in desc", methodName)
		return nil, status.Err(codes.Unimplemented, "method name %s not found in desc")
	}
	var newstm stream.Stream
	if okm {
		newstm, err = stream.NewServerStream(data, md, hc.url, service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return nil, err
		}
	} else {
		newstm, err = stream.NewServerStream(data, streamd, hc.url, service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return nil, err
		}
	}
	return newstm, nil
}

// StreamInvoke can start streaming invocation, called by triple client, with @path
func (hc *H2Controller) StreamInvoke(ctx context.Context, path string) (grpc.ClientStream, error) {
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
	headerHandler, _ := common.GetProtocolHeaderHandler(hc.url.Protocol, hc.url, ctx)
	stremaReq := h2Triple.StreamingRequest{
		SendChan: sendStreamChan,
		Handler:  headerHandler,
	}
	go func() {
		rsp, err := hc.client.Post("https://"+hc.address+path, "application/grpc+proto", &stremaReq)
		if err != nil {
			logger.Errorf("http2 request error = %s", err)
			// close send stream and return
			close(closeChan)
			return
		}
		ch := hc.readSplitData(rsp.Body)
	LOOP:
		for {
			select {
			case <-hc.closeChan:
				close(closeChan)
			case data := <-ch:
				if data.Buffer == nil || data.MsgType == message.ServerStreamCloseMsgType {
					// stream receive done, close send go routine
					close(closeChan)
					break LOOP
				}
				pkg := hc.pkgHandler.Pkg2FrameData(data.Bytes())
				clientStream.PutRecv(pkg, message.DataMsgType)
			}

		}
		trailer := rsp.Body.(*h2Triple.ResponseBody).GetTrailer()
		code, _ := strconv.Atoi(trailer.Get(codec.TrailerKeyGrpcStatus))
		msg := trailer.Get(codec.TrailerKeyGrpcMessage)
		if codes.Code(code) != codes.OK {
			logger.Errorf("grpc status not success,msg = %s, code = %d", msg, code)
		}

	}()

	pkgHandler, err := common.GetPackagerHandler(hc.url.Protocol)
	if err != nil {
		logger.Errorf("triple get package handler error = %v", err)
		return nil, err
	}
	return stream.NewClientUserStream(clientStream, serilizer, pkgHandler), nil
}

// TODO dubbo3请求发送
// UnaryInvoke can start unary invocation, called by dubbo3 client, with @path and request @data
func (hc *H2Controller) UnaryInvoke(ctx context.Context, path string, data []byte, reply interface{}) error {
	sendStreamChan := make(chan h2Triple.BufferMsg, 2)

	headerHandler, _ := common.GetProtocolHeaderHandler(hc.url.Protocol, hc.url, ctx)

	sendStreamChan <- h2Triple.BufferMsg{
		Buffer:  bytes.NewBuffer(hc.pkgHandler.Pkg2FrameData(data)),
		MsgType: h2Triple.MsgType(message.DataMsgType),
	}

	sendStreamChan <- h2Triple.BufferMsg{
		Buffer:  bytes.NewBuffer([]byte{}),
		MsgType: h2Triple.MsgType(message.ServerStreamCloseMsgType),
	}

	stremaReq := h2Triple.StreamingRequest{
		SendChan: sendStreamChan,
		Handler:  headerHandler,
	}

	// TODO 通过Http客户端发送请求，因为用的是HTTP2.0协议，所以可以偷懒
	rsp, err := hc.client.Post("https://"+hc.address+path, "application/grpc+proto", &stremaReq)
	if err != nil {
		logger.Errorf("triple unary invoke error = %v", err)
		return err
	}

	readBuf := make([]byte, hc.option.BufferSize)

	// splitBuffer is to temporarily store collected split data, and add them together
	splitBuffer := message.Message{
		Buffer: bytes.NewBuffer(make([]byte, 0)),
	}

	// todo make timeout configurable
	timeoutTicker := time.After(time.Second * 30)
	timeoutFlag := false
	readCloseChain := make(chan struct{})

	fromFrameHeaderDataSize := uint32(0)

	splitedDataChain := make(chan message.Message)

	// TODO 处理返回值
	go func() {
		for {
			select {
			case <-readCloseChain:
				return
			default:
			}
			n, err := rsp.Body.Read(readBuf)
			if err != nil {
				if err.Error() != "EOF" {
					logger.Errorf("dubbo3 unary invoke read error = %v\n", err)
					return
				}
				continue
			}
			splitedData := make([]byte, n)
			copy(splitedData, readBuf[:n])
			splitedDataChain <- message.Message{
				Buffer: bytes.NewBuffer(splitedData),
			}
		}
	}()

LOOP:
	for {
		select {
		case dataMsg := <-splitedDataChain:
			splitedData := dataMsg.Buffer.Bytes()
			if fromFrameHeaderDataSize == 0 {
				// should parse data frame header first
				var totalSize uint32
				if splitedData, totalSize = hc.pkgHandler.Frame2PkgData(splitedData); totalSize == 0 {
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
				close(readCloseChain)
				break LOOP
			}
		case <-timeoutTicker:
			// close reading loop ablove
			close(readCloseChain)
			// set timeout flag
			timeoutFlag = true
			break LOOP
		}
	}

	if timeoutFlag {
		logger.Errorf("unary call %s timeout", path)
		return perrors.Errorf("unary call %s timeout", path)
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

// Destroy destroys H2Controller and force close all related goroutine
func (hc *H2Controller) Destroy() {
	close(hc.closeChan)
}

func (hc *H2Controller) IsAvailable() bool {
	select {
	case <-hc.closeChan:
		return false
	default:
		return true
	}
	// todo check if controller's http client is available
}
