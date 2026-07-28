package host_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	corehost "github.com/EasyTier/easytier-go-host"
)

func TestPublicRPCExecutesExistingManagementMethods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := corehost.New(ctx, corehost.Options{})
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close(ctx)
	instance, err := host.CreateInstance(
		ctx,
		instanceConfig(t, 201, "10.144.0.201", 0, false, false),
	)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	defer instance.Close(ctx)
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	for _, method := range []struct {
		name           string
		index          uint64
		requirePayload bool
	}{
		{name: "ListRoute", index: 3},
		{name: "ShowNodeInfo", index: 7, requirePayload: true},
	} {
		t.Run(method.name, func(t *testing.T) {
			encodedResponse, err := instance.RPC(
				ctx,
				managementRPCRequest(method.index),
			)
			if err != nil {
				t.Fatalf("call RPC: %v", err)
			}
			if _, found, err := protobufBytesField(encodedResponse, 2); err != nil {
				t.Fatalf("decode RPC error field: %v", err)
			} else if found {
				t.Fatalf("RPC returned a method error: %x", encodedResponse)
			}
			payload, found, err := protobufBytesField(encodedResponse, 1)
			if err != nil {
				t.Fatalf("decode RPC response field: %v", err)
			}
			if method.requirePayload && (!found || len(payload) == 0) {
				t.Fatalf("RPC returned no response payload: %x", encodedResponse)
			}
		})
	}
}

func managementRPCRequest(method uint64) []byte {
	var descriptor []byte
	descriptor = appendProtobufBytes(
		descriptor,
		2,
		[]byte("PeerManageRpc"),
	)
	descriptor = appendProtobufBytes(
		descriptor,
		3,
		[]byte("PeerManageRpc"),
	)
	descriptor = appendProtobufVarint(descriptor, 4, method)

	var request []byte
	request = appendProtobufBytes(request, 1, descriptor)
	request = appendProtobufVarint(request, 3, 5_000)
	return request
}

func appendProtobufBytes(destination []byte, field uint64, value []byte) []byte {
	destination = appendProtobufRawVarint(destination, field<<3|2)
	destination = appendProtobufRawVarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendProtobufVarint(destination []byte, field, value uint64) []byte {
	destination = appendProtobufRawVarint(destination, field<<3)
	return appendProtobufRawVarint(destination, value)
}

func appendProtobufRawVarint(destination []byte, value uint64) []byte {
	for value >= 0x80 {
		destination = append(destination, byte(value)|0x80)
		value >>= 7
	}
	return append(destination, byte(value))
}

func protobufBytesField(
	message []byte,
	target uint64,
) ([]byte, bool, error) {
	for len(message) != 0 {
		key, consumed, ok := consumeProtobufVarint(message)
		if !ok {
			return nil, false, fmt.Errorf("invalid protobuf field key")
		}
		message = message[consumed:]
		field := key >> 3
		wireType := key & 7
		switch wireType {
		case 0:
			_, consumed, ok = consumeProtobufVarint(message)
			if !ok {
				return nil, false, fmt.Errorf(
					"invalid varint for field %d",
					field,
				)
			}
			message = message[consumed:]
		case 1:
			if len(message) < 8 {
				return nil, false, fmt.Errorf(
					"short fixed64 field %d",
					field,
				)
			}
			message = message[8:]
		case 2:
			length, lengthBytes, ok := consumeProtobufVarint(message)
			if !ok || length > uint64(len(message)-lengthBytes) {
				return nil, false, fmt.Errorf(
					"invalid bytes field %d",
					field,
				)
			}
			value := message[lengthBytes : uint64(lengthBytes)+length]
			if field == target {
				return value, true, nil
			}
			message = message[uint64(lengthBytes)+length:]
		case 5:
			if len(message) < 4 {
				return nil, false, fmt.Errorf(
					"short fixed32 field %d",
					field,
				)
			}
			message = message[4:]
		default:
			return nil, false, fmt.Errorf(
				"unsupported wire type %d for field %d",
				wireType,
				field,
			)
		}
	}
	return nil, false, nil
}

func consumeProtobufVarint(encoded []byte) (uint64, int, bool) {
	var value uint64
	for index, current := range encoded {
		if index == 10 || index == 9 && current > 1 {
			return 0, 0, false
		}
		value |= uint64(current&0x7f) << (7 * index)
		if current < 0x80 {
			return value, index + 1, true
		}
	}
	return 0, 0, false
}
