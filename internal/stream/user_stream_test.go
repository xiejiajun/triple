package stream

import (
	"github.com/apache/dubbo-go/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

type TestRPCService struct {
}

func (t *TestRPCService) Reference() string {
	return ""
}

func (t *TestRPCService) SetProxyImpl(impl protocol.Invoker) {

}
func (t *TestRPCService) GetProxyImpl() protocol.Invoker {
	return nil
}
func (t *TestRPCService) ServiceDesc() *grpc.ServiceDesc {
	return nil
}

func TestBaseUserStream(t *testing.T) {
	service := &TestRPCService{}
	baseUserStream := newBaseStream(service)
	assert.NotNil(t, baseUserStream)
	assert.Equal(t, baseUserStream.service, service)

	// get msg
	sendGetChan := baseUserStream.getSend()
	recvGetChan := baseUserStream.getRecv()
	closeChan := make(chan struct{})
	testData := []byte("test message of TestBaseUserStream")
	counter := 0
	go func() {
		for {
			select {
			case <-closeChan:
				// todo
				//baseUserStre//am.putRecvErr(errors.New("close error"))
				return
			case sendGetBuf := <-sendGetChan:
				counter++
				assert.Equal(t, sendGetBuf.buffer.Bytes(), testData)
				assert.Equal(t, sendGetBuf.msgType, DataMsgType)
			case recvGetBuf := <-recvGetChan:
				counter++
				assert.Equal(t, recvGetBuf.buffer.Bytes(), testData)
				assert.Equal(t, recvGetBuf.msgType, ServerStreamCloseMsgType)
			}
		}

	}()

	// put msg
	for i := 0; i < 500; i++ {
		baseUserStream.putRecv(testData, ServerStreamCloseMsgType)
		baseUserStream.putSend(testData, DataMsgType)
	}
	time.Sleep(time.Second)
	assert.Equal(t, counter, 1000)
	closeChan <- struct{}{}
	// test close with error
	//closeMsg := <-recvGetChan
	//assert.Equal(t, closeMsg.msgType, ServerStreamCloseMsgType)
	//assert.Equal(t, closeMsg.err, errors.New("close error"))
}
