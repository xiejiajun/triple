package common

type H2ControllerState uint32

const (
	DefaultMaxFrameSize         = 16384
	DefaultMaxConcurrentStreams = 100
	DefaultStreamInitWindowSize = 65535
	DefaultConnInitWindowSize   = 65535
)
