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
	"encoding/binary"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

func init() {
	common.SetPackageHandler(common.TRIPLE, NewTriplePkgHandler)
}

// TriplePackageHandler is the imple of PackageHandler, and it handles data package of triple
// e.g. now it impl as deal with pkg data as: [:5]is length and [5:lenght] is body
type TriplePackageHandler struct {
}

// Frame2PkgData/Pkg2FrameData is not as useless!
// We use it to get raw data from http2 golang package,
func (t *TriplePackageHandler) Frame2PkgData(frameData []byte) ([]byte, uint32) {
	if len(frameData) < 5 {
		return []byte{}, 0
	}
	// TODO dubbo 3.0在HTTP2协议的基础上又加了一个5字节的协议头标记数据包大小，dubbo2协议的协议头为16字节，他们的第一个字节都是标志位
	lineHeader := frameData[:5]
	length := binary.BigEndian.Uint32(lineHeader[1:])
	if len(frameData) < 5+int(length) {
		// used in streaming rpc splited header
		// we only need length of all data
		return frameData[5:], length
	}
	// TODO 返回这个TCP包的完整数据，5+length为这个TCP包的截止位置
	return frameData[5 : 5+length], length
}

// Pkg2FrameData returns data with length header
func (t *TriplePackageHandler) Pkg2FrameData(pkgData []byte) []byte {
	rsp := make([]byte, 5+len(pkgData))
	rsp[0] = byte(0)
	binary.BigEndian.PutUint32(rsp[1:], uint32(len(pkgData)))
	copy(rsp[5:], pkgData[:])
	return rsp
}

// NewTriplePkgHandler
func NewTriplePkgHandler() common.PackageHandler {
	return &TriplePackageHandler{}
}
