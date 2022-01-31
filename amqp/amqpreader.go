package amqp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func ReadFrame(reader io.Reader) (*WireFrame, error) {
	// Using little since other functions will assume big

	// get fixed size portion
	var incoming = make([]byte, 1+2+4)
	var err = binary.Read(reader, binary.LittleEndian, incoming)
	if err != nil {
		return nil, err
	}
	var memReader = bytes.NewBuffer(incoming)

	var f = &WireFrame{}

	// The reads from memReader are guaranteed to succeed because the 7 bytes
	// were allocated above and that is what is read out

	// frame type
	frameType, _ := ReadOctet(memReader)
	f.FrameType = frameType

	// channel
	channel, _ := ReadShort(memReader)
	f.Channel = channel

	// Variable length part
	var length uint32
	err = binary.Read(memReader, binary.BigEndian, &length)

	var slice = make([]byte, length+1)
	err = binary.Read(reader, binary.BigEndian, slice)
	if err != nil {
		return nil, errors.New("Bad frame payload: " + err.Error())
	}
	f.Payload = slice[0:length]
	return f, nil
}

// Fields

func ReadOctet(buf io.Reader) (data byte, err error) {
	err = binary.Read(buf, binary.BigEndian, &data)
	if err != nil {
		return 0, errors.New("Could not read byte: " + err.Error())
	}
	return data, nil
}

func ReadShort(buf io.Reader) (data uint16, err error) {
	err = binary.Read(buf, binary.BigEndian, &data)
	if err != nil {
		return 0, errors.New("Could not read uint16: " + err.Error())
	}
	return data, nil
}

func ReadLong(buf io.Reader) (data uint32, err error) {
	err = binary.Read(buf, binary.BigEndian, &data)
	if err != nil {
		return 0, errors.New("Could not read uint32: " + err.Error())
	}
	return data, nil
}

func ReadLonglong(buf io.Reader) (data uint64, err error) {
	err = binary.Read(buf, binary.BigEndian, &data)
	if err != nil {
		return 0, errors.New("Could not read uint64: " + err.Error())
	}
	return data, nil
}

func ReadShortstr(buf io.Reader) (string, error) {
	var length uint8
	var err = binary.Read(buf, binary.BigEndian, &length)
	if err != nil {
		return "", err
	}
	var slice = make([]byte, length)
	err = binary.Read(buf, binary.BigEndian, slice)
	if err != nil {
		return "", err
	}
	return string(slice), nil
}

func ReadLongstr(buf io.Reader) ([]byte, error) {
	var length uint32
	var err = binary.Read(buf, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	var slice = make([]byte, length)
	err = binary.Read(buf, binary.BigEndian, slice)
	if err != nil {
		return nil, err
	}
	return slice, err
}

// Can't get coverage on this easily since I can't currently generate
// timestamp values in Tables because protobuf doesn't give me a type that
// is different from uint64
func ReadTimestamp(buf io.Reader) (uint64, error) { // pragma: nocover
	var t uint64
	var err = binary.Read(buf, binary.BigEndian, &t)
	if err != nil { // pragma: nocover
		return 0, errors.New("Could not read uint64")
	}
	return t, nil // pragma: nocover
}

func ReadTable(reader io.Reader, strictMode bool) (*Table, error) {
	var seen = make(map[string]bool)
	var table = &Table{Table: make([]*FieldValuePair, 0)}
	var byteData, err = ReadLongstr(reader)
	if err != nil {
		return nil, errors.New("Error reading table longstr: " + err.Error())
	}
	var data = bytes.NewBuffer(byteData)
	for data.Len() > 0 {
		key, err := ReadShortstr(data)
		if err != nil {
			return nil, errors.New("Error reading key: " + err.Error())
		}
		if _, found := seen[key]; found {
			return nil, fmt.Errorf("Duplicate key in table: %s", key)
		}
		value, err := readValue(data, strictMode)
		if err != nil {
			return nil, errors.New("Error reading value for '" + key + "': " + err.Error())
		}
		table.Table = append(table.Table, &FieldValuePair{Key: &key, Value: value})
		seen[key] = true
	}
	return table, nil
}

func readValue(reader io.Reader, strictMode bool) (*FieldValue, error) {
	var t, err = ReadOctet(reader)
	if err != nil {
		return nil, err
	}

	switch {
	case t == 't':
		var v, err = ReadOctet(reader)
		if err != nil {
			return nil, err
		}
		var vb = v != 0
		return &FieldValue{Value: &FieldValue_VBoolean{VBoolean: vb}}, nil
	case t == 'b':
		var v int8
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VInt8{VInt8: v}}, nil
	case t == 'B' && strictMode:
		var v uint8
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VUint8{VUint8: v}}, nil
	case t == 'U' && strictMode || t == 's' && !strictMode:
		var v int16
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VInt16{VInt16: v}}, nil
	case t == 'u' && strictMode:
		var v uint16
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VUint16{VUint16: v}}, nil
	case t == 'I':
		var v int32
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VInt32{VInt32: v}}, nil
	case t == 'i' && strictMode:
		var v uint32
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VUint32{VUint32: v}}, nil
	case t == 'L' && strictMode || t == 'l' && !strictMode:
		var v int64
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VInt64{VInt64: v}}, nil
	case t == 'l' && strictMode:
		var v uint64
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VUint64{VUint64: v}}, nil
	case t == 'f':
		var v float32
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VFloat{VFloat: v}}, nil
	case t == 'd':
		var v float64
		if err = binary.Read(reader, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VDouble{VDouble: v}}, nil
	case t == 'D':
		var scale uint8 = 0
		var val int32 = 0
		var v = Decimal{Scale: &scale, Value: &val}
		if err = binary.Read(reader, binary.BigEndian, v.Scale); err != nil {
			return nil, err
		}
		if err = binary.Read(reader, binary.BigEndian, v.Value); err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VDecimal{VDecimal: &v}}, nil
	case t == 's' && strictMode:
		v, err := ReadShortstr(reader)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VShortstr{VShortstr: v}}, nil
	case t == 'S':
		v, err := ReadLongstr(reader)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VLongstr{VLongstr: v}}, nil
	case t == 'A':
		v, err := readArray(reader, strictMode)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VArray{VArray: &FieldArray{Value: v}}}, nil
	case t == 'T':
		v, err := ReadTimestamp(reader)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VTimestamp{VTimestamp: v}}, nil
	case t == 'F':
		v, err := ReadTable(reader, strictMode)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VTable{VTable: v}}, nil
	case t == 'V':
		return nil, nil
	case t == 'x':
		v, err := ReadLongstr(reader)
		if err != nil {
			return nil, err
		}
		return &FieldValue{Value: &FieldValue_VBytes{VBytes: v}}, nil
	}
	return nil, fmt.Errorf("Unknown table value type '%c' (%d)", t, t)
}

func readArray(reader io.Reader, strictMode bool) ([]*FieldValue, error) {
	var ret = make([]*FieldValue, 0, 0)
	var longstr, errs = ReadLongstr(reader)
	if errs != nil {
		return nil, errs
	}
	var data = bytes.NewBuffer(longstr)
	for data.Len() > 0 {
		var value, err = readValue(data, strictMode)
		if err != nil {
			return nil, err
		}
		ret = append(ret, value)
	}
	return ret, nil
}

func (props *BasicContentHeaderProperties) ReadProps(flags uint16, reader io.Reader, strictMode bool) (err error) {
	if MaskContentType&flags != 0 {
		v, err := ReadShortstr(reader)
		props.ContentType = &v
		if err != nil {
			return err
		}
	}
	if MaskContentEncoding&flags != 0 {
		v, err := ReadShortstr(reader)
		props.ContentEncoding = &v
		if err != nil {
			return err
		}
	}
	if MaskHeaders&flags != 0 {
		v, err := ReadTable(reader, strictMode)
		props.Headers = v
		if err != nil {
			return err
		}
	}
	if MaskDeliveryMode&flags != 0 {
		v, err := ReadOctet(reader)
		props.DeliveryMode = &v
		if err != nil {
			return err
		}
	}
	if MaskPriority&flags != 0 {
		v, err := ReadOctet(reader)
		props.Priority = &v
		if err != nil {
			return err
		}
	}
	if MaskCorrelationId&flags != 0 {
		v, err := ReadShortstr(reader)
		props.CorrelationId = &v
		if err != nil {
			return err
		}
	}
	if MaskReplyTo&flags != 0 {
		v, err := ReadShortstr(reader)
		props.ReplyTo = &v
		if err != nil {
			return err
		}
	}
	if MaskExpiration&flags != 0 {
		v, err := ReadShortstr(reader)
		props.Expiration = &v
		if err != nil {
			return err
		}
	}
	if MaskMessageId&flags != 0 {
		v, err := ReadShortstr(reader)
		props.MessageId = &v
		if err != nil {
			return err
		}
	}
	if MaskTimestamp&flags != 0 {
		v, err := ReadLonglong(reader)
		props.Timestamp = &v
		if err != nil {
			return err
		}
	}
	if MaskType&flags != 0 {
		v, err := ReadShortstr(reader)
		props.Type = &v
		if err != nil {
			return err
		}
	}
	if MaskUserId&flags != 0 {
		v, err := ReadShortstr(reader)
		props.UserId = &v
		if err != nil {
			return err
		}
	}
	if MaskAppId&flags != 0 {
		v, err := ReadShortstr(reader)
		props.AppId = &v
		if err != nil {
			return err
		}
	}
	if MaskReserved&flags != 0 {
		v, err := ReadShortstr(reader)
		props.Reserved = &v
		if err != nil {
			return err
		}
	}
	return nil
}
