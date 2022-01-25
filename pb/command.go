package pb

import (
	"fmt"

	"go.uber.org/zap/zapcore"
)

func (c *Command) MarshalLogObject(e zapcore.ObjectEncoder) error {
	dataLen := len(c.Data)
	if dataLen == 1 {
		e.AddString("data", "<... 1 byte>")
	} else {
		e.AddString("data", fmt.Sprintf("<... %d bytes>", dataLen))
	}
	return nil
}
