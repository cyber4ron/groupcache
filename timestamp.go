package groupcache

import (
	"encoding/binary"
	"bytes"
)

func packTimestamp(b []byte, timestamp int64) (result []byte, err error) {
	w := bytes.NewBuffer(b)
	if err := binary.Write(w, binary.LittleEndian, timestamp); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func UnpackTimestamp(b []byte) (result []byte, timestamp int64, err error) {
	if len(b) >= 8 {
		if timestamp, err = getTimestamp(b); err != nil {
			return nil, 0, err
		}
		return b[:len(b)-8], timestamp, nil
	}
	return b, 0, nil
}

func getTimestamp(b []byte) (timestamp int64, err error) {
	timestampBytes := b[len(b)-8:]
	r := bytes.NewBuffer(timestampBytes)
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}

func getTimestampByteView(bv ByteView) (timestamp int64, err error) {
	var timestampByteView ByteView
	if bv.Len() >= 8 {
		timestampByteView = bv.SliceFrom(bv.Len() - 8)
	}
	r := bytes.NewBuffer(timestampByteView.ByteSlice())
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}
