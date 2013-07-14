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
	"fmt"
	"math"
)

const (
	BUFFSIZE = 16 * 1024
)

type TBBinaryProtocol struct {
	trans           TTransport
	strictRead      bool
	strictWrite     bool
	readLength      int
	checkReadLength bool //  default false
	wbuf            *TBuffer
	rbuf            *TBuffer
}

type TBBinaryProtocolFactory struct {
	strictRead  bool
	strictWrite bool
	bufferSize  int
}

func NewTBBinaryProtocolTransport(t TTransport) *TBBinaryProtocol {
	return NewTBBinaryProtocol(t, false, true, BUFFSIZE)
}

func NewTBBinaryProtocol(t TTransport, strictRead, strictWrite bool, bufferSize int) *TBBinaryProtocol {
	rb := &TBuffer{buffer: make([]byte, bufferSize)}
	wb := &TBuffer{buffer: make([]byte, bufferSize), limit: bufferSize}
	return &TBBinaryProtocol{trans: t, strictRead: strictRead, strictWrite: strictWrite, wbuf: wb, rbuf: rb}
}

func NewTBBinaryProtocolFactoryDefault() *TBBinaryProtocolFactory {
	return NewTBBinaryProtocolFactory(false, true, BUFFSIZE)
}

func NewTBBinaryProtocolFactory(strictRead, strictWrite bool, bufferSize int) *TBBinaryProtocolFactory {
	return &TBBinaryProtocolFactory{strictRead: strictRead, strictWrite: strictWrite, bufferSize: bufferSize}
}

func (p *TBBinaryProtocolFactory) GetProtocol(t TTransport) TProtocol {
	return NewTBBinaryProtocol(t, p.strictRead, p.strictWrite, p.bufferSize)
}

/**
 * Writing Methods
 */

func (p *TBBinaryProtocol) WriteMessageBegin(name string, typeId TMessageType, seqId int32) error {
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

func (p *TBBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TBBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteFieldBegin(name string, typeId TType, id int16) error {
	e := p.WriteByte(byte(typeId))
	if e != nil {
		return e
	}
	e = p.WriteI16(id)
	return e
}

func (p *TBBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteFieldStop() error {
	e := p.WriteByte(STOP)
	return e
}

func (p *TBBinaryProtocol) WriteMapBegin(keyType TType, valueType TType, size int) error {
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

func (p *TBBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteListBegin(elemType TType, size int) error {
	e := p.WriteByte(byte(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteSetBegin(elemType TType, size int) error {
	e := p.WriteByte(byte(elemType))
	if e != nil {
		return e
	}
	e = p.WriteI32(int32(size))
	return e
}

func (p *TBBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TBBinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *TBBinaryProtocol) WriteByte(value byte) error {
	p.moreWriteSpace(1)

	p.wbuf.buffer[p.wbuf.pos] = value
	p.wbuf.pos += 1
	return nil
}

func (p *TBBinaryProtocol) WriteI16(value int16) error {
	p.moreWriteSpace(2)
	binary.BigEndian.PutUint16(p.wbuf.buffer[p.wbuf.pos:], uint16(value))
	p.wbuf.pos += 2

	return nil
}

func (p *TBBinaryProtocol) WriteI32(value int32) error {
	p.moreWriteSpace(4)
	binary.BigEndian.PutUint32(p.wbuf.buffer[p.wbuf.pos:], uint32(value))
	p.wbuf.pos += 4

	return nil
}

func (p *TBBinaryProtocol) WriteI64(value int64) error {
	p.moreWriteSpace(8)
	binary.BigEndian.PutUint64(p.wbuf.buffer[p.wbuf.pos:], uint64(value))
	p.wbuf.pos += 8

	return nil
}

func (p *TBBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(math.Float64bits(value)))
}

func (p *TBBinaryProtocol) WriteString(value string) error {
	return p.WriteBinary([]byte(value))
}

func (p *TBBinaryProtocol) WriteBinary(value []byte) error {

	p.WriteI32(int32(len(value)))

	p.moreWriteSpace(len(value))
	copy(p.wbuf.buffer[p.wbuf.pos:], value)
	p.wbuf.pos += len(value)
	return nil
}

/**
 * Reading methods
 */

func (p *TBBinaryProtocol) ReadMessageBegin() (name string, typeId TMessageType, seqId int32, err error) {
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

func (p *TBBinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadStructBegin() (name string, err error) {
	return
}

func (p *TBBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadFieldBegin() (name string, typeId TType, seqId int16, err error) {
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

func (p *TBBinaryProtocol) ReadFieldEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadMapBegin() (kType, vType TType, size int, err error) {
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

func (p *TBBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadListBegin() (elemType TType, size int, err error) {
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

func (p *TBBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadSetBegin() (elemType TType, size int, err error) {
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

func (p *TBBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TBBinaryProtocol) ReadBool() (bool, error) {
	b, e := p.ReadByte()
	v := true
	if b != 1 {
		v = false
	}
	return v, e
}

func (p *TBBinaryProtocol) ReadByte() (value byte, err error) {
	err = p.makeSureAvailable(1)
	if err == nil {
		value = p.rbuf.buffer[p.rbuf.pos]
		p.rbuf.pos += 1
	}
	return
}

func (p *TBBinaryProtocol) ReadI16() (value int16, err error) {
	err = p.makeSureAvailable(2)
	if err == nil {
		value = int16(binary.BigEndian.Uint16(p.rbuf.buffer[p.rbuf.pos:]))
		p.rbuf.pos += 2
	}
	return
}

func (p *TBBinaryProtocol) ReadI32() (value int32, err error) {
	err = p.makeSureAvailable(4)

	if err == nil {
		value = int32(binary.BigEndian.Uint32(p.rbuf.buffer[p.rbuf.pos:]))
		p.rbuf.pos += 4
	}
	return
}

func (p *TBBinaryProtocol) ReadI64() (value int64, err error) {
	err = p.makeSureAvailable(8)
	if err == nil {
		value = int64(binary.BigEndian.Uint64(p.rbuf.buffer[p.rbuf.pos:]))
		p.rbuf.pos += 8
	}
	return
}

func (p *TBBinaryProtocol) ReadDouble() (value float64, err error) {
	err = p.makeSureAvailable(8)
	if err == nil {
		value = math.Float64frombits(binary.BigEndian.Uint64(p.rbuf.buffer[p.rbuf.pos:]))
		p.rbuf.pos += 8
	}
	return
}

func (p *TBBinaryProtocol) ReadString() (value string, err error) {
	size, e := p.ReadI32()
	if e != nil {
		return "", e
	}
	return p.readStringBody(int(size))
}

func (p *TBBinaryProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}
	isize := int(size)
	err := p.makeSureAvailable(isize)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, isize)
	copy(buf, p.rbuf.buffer[p.rbuf.pos:])
	p.rbuf.pos += isize
	return buf, nil
}

func (p *TBBinaryProtocol) makeSureAvailable(space int) error {
	buf := p.rbuf
	for buf.limit-buf.pos < space {
		copy(buf.buffer, buf.buffer[buf.pos:buf.limit])

		pos := buf.pos
		buf.pos = 0
		buf.limit = buf.limit - pos

		n, err := p.trans.Read(buf.buffer[buf.limit:])
		if err != nil {
			return err
		}
		buf.limit += n
	}
	return nil
}

func (p *TBBinaryProtocol) moreWriteSpace(space int) {
	if p.wbuf.pos+space > p.wbuf.limit {
		m := p.wbuf.limit * 2
		if m < p.wbuf.pos+space {
			m = p.wbuf.pos + space
		}
		b := make([]byte, m)
		copy(b, p.wbuf.buffer[:p.wbuf.pos])
		p.wbuf = &TBuffer{buffer: b, pos: p.wbuf.pos, limit: p.wbuf.limit * 2}
	}
}

func (p *TBBinaryProtocol) Flush() (err error) {
	start := 0
	wbuf := p.wbuf
	for start < wbuf.pos {
		n, err := p.trans.Write(wbuf.buffer[start:wbuf.pos])
		if err != nil {
			return err
		}
		start += n
	}
	wbuf.pos = 0
	return NewTProtocolException(p.trans.Flush())
}

func (p *TBBinaryProtocol) Skip(fieldType TType) (err error) {
	return SkipDefaultDepth(p, fieldType)
}

func (p *TBBinaryProtocol) Transport() TTransport {
	return p.trans
}

func (p *TBBinaryProtocol) setReadLength(readLength int) {
	p.readLength = readLength
	p.checkReadLength = true
}

func (p *TBBinaryProtocol) readStringBody(size int) (value string, err error) {
	if size < 0 {
		return "", nil
	}
	err = p.makeSureAvailable(size)
	if err == nil {
		value = string(p.rbuf.buffer[p.rbuf.pos : p.rbuf.pos+size])
		p.rbuf.pos += size
	}
	return
}
