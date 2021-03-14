package buffer

import (
	"bytes"
	"github.com/dubbogo/triple/internal/status"
)

////////////////////////////////Buffer and MsgType

// BufferMsg is the basic transfer unit in one stream
type BufferMsg struct {
	Buffer  *bytes.Buffer
	MsgType MsgType
	Status  *status.Status
	Err     error // todo delete it, all change to status
}

func (bm *BufferMsg) Read(p []byte) (int, error) {
	return bm.Buffer.Read(p)
}

func (bm *BufferMsg) Bytes() []byte {
	return bm.Buffer.Bytes()
}

func (bm *BufferMsg) Write(data []byte) {
	bm.Buffer.Write(data)
}

func (bm *BufferMsg) Reset() {
	bm.Buffer.Reset()
}

func (bm *BufferMsg) Len() int {
	return bm.Buffer.Len()
}

// GetMsgType can get buffer's type
func (bm *BufferMsg) GetMsgType() MsgType {
	return bm.MsgType
}

// BufferMsgChain contain the chan of BufferMsg
type BufferMsgChain struct {
	c   chan BufferMsg
	err error
}

func NewBufferMsgChain() *BufferMsgChain {
	b := &BufferMsgChain{
		c: make(chan BufferMsg, 0),
	}
	return b
}

func (b *BufferMsgChain) Put(r BufferMsg) {
	if b.c != nil {
		b.c <- r
	}
}

func (b *BufferMsgChain) Get() <-chan BufferMsg {
	return b.c
}

func (b *BufferMsgChain) Close() {
	close(b.c)
}

/////////////////////////////////stream state
