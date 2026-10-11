package pq

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

func TestRecvMessageInvalidFrameLength(t *testing.T) {
	tests := []struct {
		name       string
		code       proto.ResponseCode
		length     int32
		wantErrMsg string
	}{
		{
			name:       "zero length",
			code:       proto.DataRow,
			length:     0,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length 0",
		},
		{
			name:       "length 1",
			code:       proto.DataRow,
			length:     1,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length 1",
		},
		{
			name:       "length 2",
			code:       proto.DataRow,
			length:     2,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length 2",
		},
		{
			name:       "length 3",
			code:       proto.DataRow,
			length:     3,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length 3",
		},
		{
			name:       "negative length -1",
			code:       proto.DataRow,
			length:     -1,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length -1",
		},
		{
			name:       "negative length -4",
			code:       proto.DataRow,
			length:     -4,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length -4",
		},
		{
			name:       "negative length -100",
			code:       proto.DataRow,
			length:     -100,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length -100",
		},
		{
			name:       "negative length min int32",
			code:       proto.DataRow,
			length:     math.MinInt32,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length -2147483648",
		},
		{
			name:       "exceeds 1gb bound",
			code:       proto.DataRow,
			length:     0x40000000,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(D) DataRow\", length 1073741824",
		},
		{
			name:       "command complete zero length",
			code:       proto.CommandComplete,
			length:     0,
			wantErrMsg: "pq: lost synchronization with server: got message type \"(C) CommandComplete\", length 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := make([]byte, 5)
			raw[0] = byte(tt.code)
			binary.BigEndian.PutUint32(raw[1:], uint32(tt.length))

			cn := &conn{
				buf: bufio.NewReader(bytes.NewReader(raw)),
			}

			var rb readBuf
			var panicVal any
			func() {
				defer func() {
					panicVal = recover()
				}()
				_, err := cn.recvMessage(&rb)
				if err == nil {
					t.Fatalf("expected error for length %d, got nil", tt.length)
				}
				if err.Error() != tt.wantErrMsg {
					t.Errorf("wrong error message:\nhave: %s\nwant: %s", err.Error(), tt.wantErrMsg)
				}
			}()

			if panicVal != nil {
				t.Fatalf("unexpected panic: %v", panicVal)
			}
			if err := cn.err.get(); !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("connection error not set to ErrBadConn, got %v", err)
			}
		})
	}
}

func TestReadBuf(t *testing.T) {
	t.Run("basic types", func(t *testing.T) {
		data := []byte{
			0x00, 0x00, 0x00, 0x2a, // int32: 42
			0x00, 0x00, 0x04, 0xd2, // oid: 1234
			0x00, 0x10, // int16: 16
			'h', 'e', 'l', 'l', 'o', 0x00, // string: "hello"
			0x01, 0x02, 0x03, // next(3)
			0x7f, // byte
		}

		b := readBuf(data)
		if v := b.int32(); v != 42 {
			t.Errorf("int32: have %d, want 42", v)
		}
		if v := b.oid(); v != oid.Oid(1234) {
			t.Errorf("oid: have %d, want 1234", v)
		}
		if v := b.int16(); v != 16 {
			t.Errorf("int16: have %d, want 16", v)
		}
		if v := b.string(); v != "hello" {
			t.Errorf("string: have %q, want hello", v)
		}
		if v := b.next(3); !bytes.Equal(v, []byte{0x01, 0x02, 0x03}) {
			t.Errorf("next: have %v, want [1 2 3]", v)
		}
		if v := b.byte(); v != 0x7f {
			t.Errorf("byte: have 0x%x, want 0x7f", v)
		}
		if len(b) != 0 {
			t.Errorf("expected empty buffer, remaining: %d", len(b))
		}
	})

	t.Run("string without null terminator panics", func(t *testing.T) {
		b := readBuf([]byte("unterminated"))
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic for unterminated string")
			}
		}()
		b.string()
	})
}

func TestWriteBuf(t *testing.T) {
	var wb writeBuf
	wb.byte(proto.Query)
	wb.int32(100)
	wb.int16(20)
	wb.string("test")
	wb.bytes([]byte{1, 2})

	if wb.buf[0] != byte(proto.Query) {
		t.Errorf("first byte: have %c, want %c", wb.buf[0], proto.Query)
	}
	res := wb.wrap()
	if len(res) == 0 {
		t.Fatal("wrap returned empty buffer")
	}
}
