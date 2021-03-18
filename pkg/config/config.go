package config

import "github.com/dubbogo/triple/pkg/common"

type Option struct {
	Timeout    uint32
	BufferSize uint32
}

// SetEmptyFieldDefaultConfig set empty field to default config
func (o *Option) SetEmptyFieldDefaultConfig() {
	if o.Timeout == uint32(0) {
		o.Timeout = uint32(common.DefaultTimeout)
	}

	if o.BufferSize == uint32(0) {
		o.BufferSize = uint32(common.DefaultHttp2ControllerReadBufferSize)
	}
}

type OptionFunction func(o *Option) *Option

// NewTripleOption return Triple Option with given config defined by @fs
func NewTripleOption(fs ...OptionFunction) *Option {
	opt := &Option{}
	for _, v := range fs {
		opt = v(opt)
	}
	return opt
}

// WithClientTimeout return OptionFunction with timeout of @timeout
func WithClientTimeout(timeout uint32) OptionFunction {
	return func(o *Option) *Option {
		o.Timeout = timeout
		return o
	}
}

// WithBufferSize return OptionFunction with buffer read size of @size
func WithBufferSize(size uint32) OptionFunction {
	return func(o *Option) *Option {
		o.BufferSize = size
		return o
	}
}
