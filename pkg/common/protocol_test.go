package common

import (
	"context"
	"github.com/apache/dubbo-go/common"
	netTriple "github.com/dubbogo/net/http2/triple"
	"gotest.tools/assert"
	"net/http"
	"reflect"
	"testing"
)

type ImplProtocolHeader struct {
	Method   string
	StreamID uint32
}

func (t *ImplProtocolHeader) GetPath() string {
	return t.Method
}
func (t *ImplProtocolHeader) GetStreamID() uint32 {
	return t.StreamID
}

// FieldToCtx parse triple Header that user defined, to ctx of server end
func (t *ImplProtocolHeader) FieldToCtx() context.Context {
	return context.Background()
}

type ImplProtocolHeaderHandler struct {
}

func (ihh *ImplProtocolHeaderHandler) ReadFromTripleReqHeader(header *http.Request) netTriple.ProtocolHeader {
	return &ImplProtocolHeader{}
}

func (hh *ImplProtocolHeaderHandler) WriteTripleReqHeaderField(header http.Header) http.Header {
	return nil
}

func (hh *ImplProtocolHeaderHandler) WriteTripleFinalRspHeaderField(w http.ResponseWriter, grpcStatusCode int, grpcMessage string, traceProtoBin int) {

}

func NewTestHeaderHandler(url *common.URL, ctx context.Context) netTriple.ProtocolHeaderHandler {
	return &ImplProtocolHeaderHandler{}
}

func TestSetAndGetProtocolHeaderHandler(t *testing.T) {
	oriHandler := NewTestHeaderHandler(nil, context.Background())
	SetProtocolHeaderHandler("test-protocol", NewTestHeaderHandler)
	handler, err := GetProtocolHeaderHandler("test-protocol", nil, context.Background())
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(handler), reflect.TypeOf(oriHandler))
}

type TestTriplePackageHandler struct {
}

func (t *TestTriplePackageHandler) Frame2PkgData(frameData []byte) ([]byte, uint32) {
	return frameData, 0
}
func (t *TestTriplePackageHandler) Pkg2FrameData(pkgData []byte) []byte {
	return pkgData
}

func newTestTriplePackageHandler() PackageHandler {
	return &TestTriplePackageHandler{}
}

func TestSetAndGetGetPackagerHandler(t *testing.T) {
	oriHandler := newTestTriplePackageHandler()
	SetPackageHandler("test-protocol", newTestTriplePackageHandler)
	handler, err := GetPackagerHandler("test-protocol")
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(handler), reflect.TypeOf(oriHandler))
}

type TestDubbo3Serializer struct {
}

func (p *TestDubbo3Serializer) Marshal(v interface{}) ([]byte, error) {
	return []byte{}, nil
}
func (p *TestDubbo3Serializer) Unmarshal(data []byte, v interface{}) error {
	return nil
}

func newTestDubbo3Serializer() Dubbo3Serializer {
	return &TestDubbo3Serializer{}
}

func TestGetAndSetSerilizer(t *testing.T) {
	oriSerializer := newTestDubbo3Serializer()
	SetDubbo3Serializer("test-protocol", newTestDubbo3Serializer)
	ser, err := GetDubbo3Serializer("test-protocol")
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(ser), reflect.TypeOf(oriSerializer))
}
