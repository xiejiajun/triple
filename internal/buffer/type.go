package buffer

// MsgType show the type of Message in buffer
type MsgType uint8

const (
	DataMsgType              = MsgType(1)
	ServerStreamCloseMsgType = MsgType(2)
)
