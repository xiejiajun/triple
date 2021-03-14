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

package stream

import (
	"bytes"
	h2Triple "github.com/dubbogo/net/http2/triple"
	"github.com/dubbogo/triple/internal/buffer"
)
import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"google.golang.org/grpc"
)
import (
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/common"
)

/////////////////////////////////stream
// Stream is not only a buffer stream
// but an abstruct stream in h2 defination
type Stream interface {
	// channel usage
	PutRecv(data []byte, msgType buffer.MsgType)
	PutSend(data []byte, msgType buffer.MsgType)
	GetSend() <-chan buffer.BufferMsg
	GetRecv() <-chan buffer.BufferMsg
	PutSplitedDataRecv(splitedData []byte, msgType buffer.MsgType, handler common.PackageHandler)
	Close()
}

// baseStream is the basic  impl of stream interface, it impl for basic function of stream
type baseStream struct {
	recvBuf *buffer.BufferMsgChain
	sendBuf *buffer.BufferMsgChain
	service common.Dubbo3GrpcService
	// splitBuffer is used to cache splited data from network, if exceed
	splitBuffer buffer.BufferMsg
	// fromFrameHeaderDataSize is got from dataFrame's header, which is 5 bytes and contains the total data size
	// of this package
	// when fromFrameHeaderDataSize is zero, its means we should parse header first 5byte, and then read data
	fromFrameHeaderDataSize uint32
}

func (s *baseStream) WriteCloseMsgTypeWithStatus(st *status.Status) {
	s.sendBuf.Put(buffer.BufferMsg{
		Status:  st,
		MsgType: buffer.ServerStreamCloseMsgType,
	})
}

func (s *baseStream) PutRecv(data []byte, msgType buffer.MsgType) {
	s.recvBuf.Put(buffer.BufferMsg{
		Buffer:  bytes.NewBuffer(data),
		MsgType: msgType,
	})
}

// putSplitedDataRecv is called when receive from tripleNetwork, dealing with big package partial to create the whole pkg
// @msgType Must be data
func (s *baseStream) PutSplitedDataRecv(splitedData []byte, msgType buffer.MsgType, frameHandler common.PackageHandler) {
	if msgType != buffer.DataMsgType {
		return
	}
	if s.fromFrameHeaderDataSize == 0 {
		// should parse data frame header first
		var totalSize uint32
		if splitedData, totalSize = frameHandler.Frame2PkgData(splitedData); totalSize == 0 {
			return
		} else {
			s.fromFrameHeaderDataSize = totalSize
		}
		s.splitBuffer.Reset()
	}
	s.splitBuffer.Write(splitedData)
	if s.splitBuffer.Len() > int(s.fromFrameHeaderDataSize) {
		panic("Receive Splited Data is bigger than wanted!!!")
		return
	}

	if s.splitBuffer.Len() == int(s.fromFrameHeaderDataSize) {
		s.PutRecv(frameHandler.Pkg2FrameData(s.splitBuffer.Bytes()), msgType)
		s.splitBuffer.Reset()
		s.fromFrameHeaderDataSize = 0
	}
}

func (s *baseStream) PutSend(data []byte, msgType buffer.MsgType) {
	s.sendBuf.Put(buffer.BufferMsg{
		Buffer:  bytes.NewBuffer(data),
		MsgType: msgType,
	})
}

// getRecv get channel of receiving message
// in server end, when unary call, msg from client is send to recvChan, and then it is read and push to processor to get response.
// in client end, when unary call, msg from server is send to recvChan, and then response in invoke method.
/*
client  ---> send chan ---> triple ---> recv Chan ---> processor
			sendBuf						recvBuf			   |
		|	clientStream |          | serverStream |       |
			recvBuf						sendBuf			   V
client <--- recv chan <--- triple <--- send chan <---  response
*/
func (s *baseStream) GetRecv() <-chan buffer.BufferMsg {
	return s.recvBuf.Get()
}

func (s *baseStream) GetSend() <-chan buffer.BufferMsg {
	return s.sendBuf.Get()
}

func (s *baseStream) Close() {
	s.recvBuf.Close()
	s.sendBuf.Close()
}

func newBaseStream(service common.Dubbo3GrpcService) *baseStream {
	// stream and pkgHeader are the same level
	return &baseStream{
		recvBuf: buffer.NewBufferMsgChain(),
		sendBuf: buffer.NewBufferMsgChain(),
		service: service,
		splitBuffer: buffer.BufferMsg{
			Buffer: bytes.NewBuffer(make([]byte, 0)),
		},
	}
}

// ServerStream is running in server end
type ServerStream struct {
	baseStream
	processor processor
	header    h2Triple.ProtocolHeader
}

func (ss *ServerStream) Close() {
	// close processor, as there may be rpc call that is waiting for process, let them returns canceled code
	ss.processor.close()
}

func NewServerStream(header h2Triple.ProtocolHeader, desc interface{}, url *dubboCommon.URL, service common.Dubbo3GrpcService) (*ServerStream, error) {
	baseStream := newBaseStream(service)

	serverStream := &ServerStream{
		baseStream: *baseStream,
		header:     header,
	}
	pkgHandler, err := common.GetPackagerHandler(url.Protocol)
	if err != nil {
		logger.Error("GetPkgHandler error with err = ", err)
		return nil, err
	}
	if methodDesc, ok := desc.(grpc.MethodDesc); ok {
		// pkgHandler and processor are the same level
		serverStream.processor, err = newUnaryProcessor(serverStream, pkgHandler, methodDesc)
	} else if streamDesc, ok := desc.(grpc.StreamDesc); ok {
		serverStream.processor, err = newStreamingProcessor(serverStream, pkgHandler, streamDesc)
	} else {
		logger.Error("grpc desc invalid:", desc)
		return nil, nil
	}

	serverStream.processor.runRPC()

	return serverStream, nil
}

func (s *ServerStream) getService() common.Dubbo3GrpcService {
	return s.service
}

func (s *ServerStream) getHeader() h2Triple.ProtocolHeader {
	return s.header
}

// clientStream is running in client end
type clientStream struct {
	baseStream
}

func NewClientStream() *clientStream {
	baseStream := newBaseStream(nil)
	newclientStream := &clientStream{
		baseStream: *baseStream,
	}
	return newclientStream
}

// todo close logic
func (cs *clientStream) Close() {
	cs.baseStream.Close()
}
