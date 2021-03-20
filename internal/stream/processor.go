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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or codecied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stream

import (
	"bytes"
	"errors"
	"sync"
)
import (
	"github.com/apache/dubbo-go/common/logger"
	h2Triple "github.com/dubbogo/net/http2/triple"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)
import (
	"github.com/dubbogo/triple/internal/codec"
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/internal/message"
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/common"
)

// processor is the interface, with func runRPC and close
// it process server RPC method that user defined and get response
type processor interface {
	runRPC()
	close()
}

// baseProcessor is the basic impl of processor, which contains four base fields, such as rpc status handle function
type baseProcessor struct {
	stream     *serverStream
	pkgHandler common.PackageHandler
	serializer common.Dubbo3Serializer
	closeChain chan struct{}
	quitOnce   sync.Once
}

func (p *baseProcessor) handleRPCErr(err error) {
	appStatus, ok := status.FromError(err)
	if !ok {
		err = status.Errorf(codes.Unknown, err.Error())
		appStatus, _ = status.FromError(err)
	}
	p.stream.WriteCloseMsgTypeWithStatus(appStatus)
}

// handleRPCSuccess send data and grpc success code with message
func (p *baseProcessor) handleRPCSuccess(data []byte) {
	p.stream.PutSend(data, message.DataMsgType)
	p.stream.WriteCloseMsgTypeWithStatus(status.New(codes.OK, ""))
}

// close closes processor once
func (p *baseProcessor) close() {
	p.quitOnce.Do(func() {
		close(p.closeChain)
	})
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
		logger.Error("newProcessor with serialization"+
			" ", codec.DefaultDubbo3SerializerName, " error")
		return nil, err
	}

	return &unaryProcessor{
		baseProcessor: baseProcessor{
			serializer: serilizer,
			stream:     s,
			pkgHandler: pkgHandler,
			closeChain: make(chan struct{}, 1),
			quitOnce:   sync.Once{},
		},
		methodDesc: desc,
	}, nil
}

// processUnaryRPC can process unary rpc
func (p *unaryProcessor) processUnaryRPC(buf bytes.Buffer, service common.Dubbo3GrpcService, header h2Triple.ProtocolHeader) ([]byte, error) {
	readBuf := buf.Bytes()

	pkgData, _ := p.pkgHandler.Frame2PkgData(readBuf)

	descFunc := func(v interface{}) error {
		if err := p.serializer.Unmarshal(pkgData, v.(proto.Message)); err != nil {
			return status.Errorf(codes.Internal, "Unary rpc request unmarshal error: %s", err)
		}
		return nil
	}

	reply, err := p.methodDesc.Handler(service, header.FieldToCtx(), descFunc, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unary rpc handle error: %s", err)
	}

	replyData, err := p.serializer.Marshal(reply.(proto.Message))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unary rpc reoly marshal error: %s", err)
	}

	rspFrameData := p.pkgHandler.Pkg2FrameData(replyData)
	return rspFrameData, nil
}

// runRPC is called by lower layer's stream
func (p *unaryProcessor) runRPC() {
	recvChan := p.stream.GetRecv()
	go func() {
		select {
		case <-p.closeChain:
			// in this case, server doesn't receive data but got close signal, it returns canceled code
			logger.Warn("unaryProcessor closed by force")
			p.handleRPCErr(status.Errorf(codes.Canceled, "processor has been canceled!"))
			return
		case recvMsg := <-recvChan:
			// in this case, server unary processor have the chance to do process and return result
			defer func() {
				if e := recover(); e != nil {
					p.handleRPCErr(errors.New(e.(string)))
				}
			}()
			if recvMsg.Err != nil {
				logger.Error("error ,s.processUnaryRPC err = ", recvMsg.Err)
				p.handleRPCErr(status.Errorf(codes.Internal, "error ,s.processUnaryRPC err = %s", recvMsg.Err))
				return
			}
			rspData, err := p.processUnaryRPC(*recvMsg.Buffer, p.stream.getService(), p.stream.getHeader())
			if err != nil {
				p.handleRPCErr(err)
				return
			}

			// TODO: status sendResponse should has err, then writeStatus(err) use one function and defer
			// it's enough that unary processor just send data msg to stream layer
			// rpc status logic just let stream layer to handle
			p.handleRPCSuccess(rspData)
			return
		}
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
			closeChain: make(chan struct{}, 1),
			quitOnce:   sync.Once{},
		},
		streamDesc: desc,
	}, nil
}

// runRPC called by stream
func (sp *streamingProcessor) runRPC() {
	serverUserstream := newServerUserStream(sp.stream, sp.serializer, sp.pkgHandler)
	go func() {
		// TODO gRpc的Stream Handler
		if err := sp.streamDesc.Handler(sp.stream.getService(), serverUserstream); err != nil {
			sp.handleRPCErr(err)
			return
		}
		// for stream rpc, processor should send CloseMsg to lower stream layer to call close
		// but unary rpc not, unary rpc processor only send data to stream layer
		sp.handleRPCSuccess(nil)
	}()
}
