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
	codec "github.com/dubbogo/triple/codec"

)
import (
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)
import (
	dubboCommon"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/dubbogo/triple/common"
)

// processor is the common, with func runRPC
type processor interface {
	runRPC()
}

// baseProcessor is the basic impl of porcessor, which contains four base fields
type baseProcessor struct {
	stream     *serverStream
	pkgHandler common.PackageHandler
	serializer common.Dubbo3Serializer
}

// unaryProcessor used to process unary invocation
type unaryProcessor struct {
	baseProcessor
	methodDesc grpc.MethodDesc
}

// newUnaryProcessor can create unary processor
func newUnaryProcessor(s *serverStream, pkgHandler common.PackageHandler, desc grpc.MethodDesc) (processor, error) {
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("newProcessor with serlizationg ", codec.DefaultDubbo3SerializerName, " error")
		return nil, err
	}

	return &unaryProcessor{
		baseProcessor: baseProcessor{
			serializer: serilizer,
			stream:     s,
			pkgHandler: pkgHandler,
		},
		methodDesc: desc,
	}, nil
}

// processUnaryRPC can process unary rpc
func (p *unaryProcessor) processUnaryRPC(buf bytes.Buffer, service dubboCommon.RPCService, header common.ProtocolHeader) ([]byte, error) {
	readBuf := buf.Bytes()

	pkgData := p.pkgHandler.Frame2PkgData(readBuf)

	descFunc := func(v interface{}) error {
		if err := p.serializer.Unmarshal(pkgData, v.(proto.Message)); err != nil {
			return err
		}
		return nil
	}

	reply, err := p.methodDesc.Handler(service, header.FieldToCtx(), descFunc, nil)
	if err != nil {
		return nil, err
	}

	replyData, err := p.serializer.Marshal(reply.(proto.Message))
	if err != nil {
		return nil, err
	}

	rspFrameData := p.pkgHandler.Pkg2FrameData(replyData)
	return rspFrameData, nil
}

// runRPC is called by lower layer's stream
func (s *unaryProcessor) runRPC() {
	recvChan := s.stream.getRecv()
	go func() {
		recvMsg := <-recvChan
		if recvMsg.err != nil {
			return
		}
		rspData, err := s.processUnaryRPC(*recvMsg.buffer, s.stream.getService(), s.stream.getHeader())
		if err != nil {
			logger.Error("error ,s.processUnaryRPC err = ", err)
			return
		}
		s.stream.putSend(rspData, DataMsgType)
	}()
}

// streamingProcessor used to process streaming invocation
type streamingProcessor struct {
	baseProcessor
	streamDesc grpc.StreamDesc
}

// newStreamingProcessor can create new streaming processor
func newStreamingProcessor(s *serverStream, pkgHandler common.PackageHandler, desc grpc.StreamDesc) (processor, error) {
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("newProcessor with serlizationg ", codec.DefaultDubbo3SerializerName, " error")
		return nil, err
	}

	return &streamingProcessor{
		baseProcessor: baseProcessor{
			serializer: serilizer,
			stream:     s,
			pkgHandler: pkgHandler,
		},
		streamDesc: desc,
	}, nil
}

// runRPC called by stream
func (sp *streamingProcessor) runRPC() {
	serverUserstream := newServerUserStream(sp.stream, sp.serializer, sp.pkgHandler)
	go func() {
		sp.streamDesc.Handler(sp.stream.getService(), serverUserstream)
		sp.stream.putSend(nil, ServerStreamCloseMsgType)
	}()
}
