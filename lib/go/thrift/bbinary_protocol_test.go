package thrift

import (
	"testing"
)

func TestReadWriteBBinaryProtocol(t *testing.T) {
	ReadWriteProtocolTest(t, NewTBBinaryProtocolFactoryDefault())
}
