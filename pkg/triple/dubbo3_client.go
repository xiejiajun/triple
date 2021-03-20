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
	"context"
	"github.com/dubbogo/triple/pkg/config"
	"reflect"
	"sync"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// TripleClient client endpoint for client end
type TripleClient struct {
	h2Controller *H2Controller
	addr         string
	Invoker      reflect.Value
	url          *dubboCommon.URL

	//once is used when destroy
	once sync.Once

	// triple config
	opt *config.Option
}

// NewTripleClient create triple client with given @url,
// it's return tripleClient , contains invoker, and contain triple conn
// @url is the invocation url when dubbo client invoct. Now, triple only use Location and Protocol field of url.
// @impl must have method: GetDubboStub(cc *dubbo3.TripleConn) interface{}, to be capable with grpc
// @opt is used init http2 controller, if it's nil, use the default config
func NewTripleClient(url *dubboCommon.URL, impl interface{}, opt *config.Option) (*TripleClient, error) {
	tripleClient := &TripleClient{
		url: url,
		opt: opt,
	}
	// start triple client connection,
	if err := tripleClient.connect(url); err != nil {
		return nil, err
	}

	// put dubbo3 network logic to tripleConn.
	invoker := getInvoker(impl, newTripleConn(tripleClient))

	// put dubbo3 network logic to tripleClient
	tripleClient.Invoker = reflect.ValueOf(invoker)

	return tripleClient, nil
}

// Connect called when new TripleClient, which start a tcp conn with target addr
func (t *TripleClient) connect(url *dubboCommon.URL) error {
	logger.Info("want to connect to url = ", url.Location)
	t.addr = url.Location
	var err error
	t.h2Controller, err = NewH2Controller(false, nil, url, t.opt)
	if err != nil {
		logger.Errorf("dubbo client new http2 controller error = %v", err)
		return err
	}
	t.h2Controller.address = url.Location
	return nil
}

// Request call h2Controller to send unary rpc req to server
// @path is /interfaceKey/functionName e.g. /com.apache.dubbo.sample.basic.IGreeter/BigUnaryTest
// @arg is request body
func (t *TripleClient) Request(ctx context.Context, path string, arg, reply proto.Message) error {
	reqData, err := proto.Marshal(arg)
	if err != nil {
		logger.Errorf("client request marshal error = %v", err)
		return err
	}
	if t.h2Controller == nil {
		if err := t.connect(t.url); err != nil {
			logger.Errorf("dubbo client connect to url error = %v", err)
			return err
		}
	}
	// TODO 客户端发请求
	if err := t.h2Controller.UnaryInvoke(ctx, path, reqData, reply); err != nil {
		return err
	}
	return nil
}

// StreamRequest call h2Controller to send streaming request to sever, to start link.
// @path is /interfaceKey/functionName e.g. /com.apache.dubbo.sample.basic.IGreeter/BigStreamTest
func (t *TripleClient) StreamRequest(ctx context.Context, path string) (grpc.ClientStream, error) {
	if t.h2Controller == nil {
		if err := t.connect(t.url); err != nil {
			logger.Errorf("dubbo client connect to url error = %v", err)
			return nil, err
		}
	}
	return t.h2Controller.StreamInvoke(ctx, path)
}

// Close destroy http controller and return
func (t *TripleClient) Close() {
	logger.Debug("Triple Client Is closing")
	t.h2Controller.Destroy()
}

// IsAvailable returns if ht
func (t *TripleClient) IsAvailable() bool {
	if t.h2Controller == nil {
		return false
	}
	return t.h2Controller.IsAvailable()
}
