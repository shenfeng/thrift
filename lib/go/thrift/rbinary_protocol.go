/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var noMoreData error = errors.New("No more data")

// read from memory, no copy
type TRBinaryProtocol struct {
	buffer      []byte
	pos         int
	strictRead  bool
	strictWrite bool
	readOnly    bool
}

// Read from buffer, decode using binary protocol. avoid memory copy
func NewTRBinaryProtocol(buffer []byte) *TRBinaryProtocol {
	return &TRBinaryProtocol{buffer: buffer, strictRead: false, readOnly: true}
}

// write to a internal buffer, using Bytes() to get it
func NewTWBinaryProtocol(bufferSize int) *TRBinaryProtocol {
	return &TRBinaryProtocol{buffer: make([]byte, bufferSize), strictWrite: false}
}

func (p *TRBinaryProtocol) Bytes() []byte {
	return p.buffer[:p.pos]
}

/**
 * Writing Methods
 */

func (p *TRBinaryProtocol) WriteMessageBegin(name string, typeId TMessageType, seqId int32) error {
	if p.readOnly {
		panic("Read only")
	}
	if p.strictWrite {
		version := uint32(VERSION_1) | uint32(typeId)
		e := p.WriteI32(int32(version))
		if e != nil {
			return e
		}
		e = p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	} else {
		e := p.WriteString(name)
		if e != nil {
			return e
		}
		e = p.WriteByte(byte(typeId))
		if e != nil {
			return e
		}
		e = p.WriteI32(seqId)
		return e
	}
	return nil
}

func (p *TRBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TRBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteFieldBegin(name string, typeId TType, id int16) error {
	e := p.WriteByte(byte(typeId))
	if e != nil {
		return e
	}
	e = p.WriteI16(id)
	return e
}

func (p *TRBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteFieldStop() error {
	e := p.WriteByte(STOP)
	return e
}

func (p *TRBinaryProtocol) WriteMapBegin(keyType TType, valueType TType, size int) error {
	e := p.WriteByte(byte(keyType))
	if e != nil {
		return e
	}
	e = p.WriteByte(byte(valueType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TRBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteListBegin(elemType TType, size int) error {
	e := p.WriteByte(byte(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TRBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteSetBegin(elemType TType, size int) error {
	e := p.WriteByte(byte(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TRBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TRBinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *TRBinaryProtocol) WriteByte(value byte) error {
	p.moreWriteSpace(1)
	p.buffer[p.pos] = value
	p.pos += 1
	return nil
}

func (p *TRBinaryProtocol) WriteI16(value int16) error {
	p.moreWriteSpace(2)
	binary.BigEndian.PutUint16(p.buffer[p.pos:], uint16(value))
	p.pos += 2
	return nil
}

func (p *TRBinaryProtocol) WriteI32(value int32) error {
	p.moreWriteSpace(4)
	binary.BigEndian.PutUint32(p.buffer[p.pos:], uint32(value))
	p.pos += 4
	return nil
}

func (p *TRBinaryProtocol) WriteI64(value int64) error {
	p.moreWriteSpace(8)
	binary.BigEndian.PutUint64(p.buffer[p.pos:], uint64(value))
	p.pos += 8
	return nil
}

func (p *TRBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(math.Float64bits(value)))
}

func (p *TRBinaryProtocol) WriteString(value string) error {
	return p.WriteBinary([]byte(value))
}

func (p *TRBinaryProtocol) WriteBinary(value []byte) error {

	p.WriteI32(int32(len(value)))

	p.moreWriteSpace(len(value))
	copy(p.buffer[p.pos:], value)
	p.pos += len(value)
	return nil
}

/**
 * Reading methods
 */

func (p *TRBinaryProtocol) ReadMessageBegin() (name string, typeId TMessageType, seqId int32, err error) {
	if !p.readOnly {
		panic("Write only")
	}

	size, e := p.ReadI32()
	if e != nil {
		return "", typeId, 0, NewTProtocolException(e)
	}
	if size < 0 {
		typeId = TMessageType(size & 0x0ff)
		version := int64(int64(size) & VERSION_MASK)
		if version != VERSION_1 {
			return name, typeId, seqId, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Bad version in ReadMessageBegin"))
		}
		name, e = p.ReadString()
		if e != nil {
			return name, typeId, seqId, NewTProtocolException(e)
		}
		seqId, e = p.ReadI32()
		if e != nil {
			return name, typeId, seqId, NewTProtocolException(e)
		}
		return name, typeId, seqId, nil
	}
	if p.strictRead {
		return name, typeId, seqId, NewTProtocolExceptionWithType(BAD_VERSION, fmt.Errorf("Missing version in ReadMessageBegin"))
	}
	name, e2 := p.readStringBody(int(size))
	if e2 != nil {
		return name, typeId, seqId, e2
	}
	b, e3 := p.ReadByte()
	if e3 != nil {
		return name, typeId, seqId, e3
	}
	typeId = TMessageType(b)
	seqId, e4 := p.ReadI32()
	if e4 != nil {
		return name, typeId, seqId, e4
	}
	return name, typeId, seqId, nil
}

func (p *TRBinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadStructBegin() (name string, err error) {
	return
}

func (p *TRBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadFieldBegin() (name string, typeId TType, seqId int16, err error) {
	t, err := p.ReadByte()
	typeId = TType(t)
	if err != nil {
		return name, typeId, seqId, err
	}
	if t != STOP {
		seqId, err = p.ReadI16()
	}
	return name, typeId, seqId, err
}

func (p *TRBinaryProtocol) ReadFieldEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadMapBegin() (kType, vType TType, size int, err error) {
	k, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	kType = TType(k)
	v, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	vType = TType(v)
	size32, e := p.ReadI32()
	size = int(size32)
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	return kType, vType, size, nil
}

func (p *TRBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadListBegin() (elemType TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	elemType = TType(b)
	size32, e := p.ReadI32()
	size = int(size32)
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	return elemType, size, nil
}

func (p *TRBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadSetBegin() (elemType TType, size int, err error) {
	b, e := p.ReadByte()
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	elemType = TType(b)
	size32, e := p.ReadI32()
	size = int(size32)
	if e != nil {
		err = NewTProtocolException(e)
		return
	}
	return elemType, size, nil
}

func (p *TRBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TRBinaryProtocol) ReadBool() (bool, error) {
	b, e := p.ReadByte()
	v := true
	if b != 1 {
		v = false
	}
	return v, e
}

func (p *TRBinaryProtocol) ReadByte() (value byte, err error) {
	if p.pos+1 > len(p.buffer) {
		err = noMoreData
		return
	}
	value = p.buffer[p.pos]
	p.pos += 1
	return
}

func (p *TRBinaryProtocol) ReadI16() (value int16, err error) {
	if p.pos+2 > len(p.buffer) {
		err = noMoreData
		return
	}
	value = int16(binary.BigEndian.Uint16(p.buffer[p.pos:]))
	p.pos += 2
	return
}

func (p *TRBinaryProtocol) ReadI32() (value int32, err error) {
	if p.pos+4 > len(p.buffer) {
		err = noMoreData
		return
	}
	value = int32(binary.BigEndian.Uint32(p.buffer[p.pos:]))
	p.pos += 4
	return
}

func (p *TRBinaryProtocol) ReadI64() (value int64, err error) {
	if p.pos+8 > len(p.buffer) {
		err = noMoreData
		return
	}
	value = int64(binary.BigEndian.Uint64(p.buffer[p.pos:]))
	p.pos += 8
	return
}

func (p *TRBinaryProtocol) ReadDouble() (value float64, err error) {
	if p.pos+8 > len(p.buffer) {
		err = noMoreData
		return
	}
	value = math.Float64frombits(binary.BigEndian.Uint64(p.buffer[p.pos:]))
	p.pos += 8
	return
}

func (p *TRBinaryProtocol) ReadString() (value string, err error) {
	var size int32
	size, err = p.ReadI32()
	if err != nil {
		return
	}
	return p.readStringBody(int(size))
}

func (p *TRBinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	isize := int(size)
	if p.pos+isize > len(p.buffer) {
		return nil, noMoreData
	}

	buf := make([]byte, isize)
	copy(buf, p.buffer[p.pos:])
	p.pos += isize
	return buf, nil
}

func (p *TRBinaryProtocol) moreWriteSpace(space int) {
	if p.pos+space > len(p.buffer) {
		m := len(p.buffer) * 2
		if m < p.pos+space {
			m = p.pos + space
		}
		b := make([]byte, m)
		copy(b, p.buffer)
		p.buffer = b
	}
}

func (p *TRBinaryProtocol) Flush() (err error) {
	return nil
}

func (p *TRBinaryProtocol) Skip(fieldType TType) (err error) {
	return SkipDefaultDepth(p, fieldType)
}

func (p *TRBinaryProtocol) Transport() TTransport {
	panic("Not supported")
}

func (p *TRBinaryProtocol) readStringBody(size int) (value string, err error) {
	if size < 0 {
		return "", nil
	}
	if p.pos+size > len(p.buffer) {
		return "", noMoreData
	}
	if err == nil {
		value = string(p.buffer[p.pos : p.pos+size])
		p.pos += size
	}
	return
}
