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

package codec

import (
	"github.com/golang/protobuf/proto"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

func init() {
	common.SetDubbo3Serializer(DefaultDubbo3SerializerName, NewProtobufCodeC)
}

// DefaultDubbo3SerializerName is the default serializer name, triple use pb as serializer.
const DefaultDubbo3SerializerName = "protobuf"

// ProtobufCodeC is the protobuf impl of Dubbo3Serializer interface
type ProtobufCodeC struct {
}

// Marshal serialize interface @v to bytes
func (p *ProtobufCodeC) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

// Unmarshal deserialize @data to interface
func (p *ProtobufCodeC) Unmarshal(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

// NewProtobufCodeC returns new ProtobufCodeC
func NewProtobufCodeC() common.Dubbo3Serializer {
	return &ProtobufCodeC{}
}
