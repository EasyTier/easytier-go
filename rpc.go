package host

import (
	"context"
	"fmt"
	"time"

	apiinstance "github.com/EasyTier/easytier-go-host/proto/api/instance"
	"github.com/EasyTier/easytier-go-host/proto/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const defaultRPCTimeout = 5 * time.Second

var (
	listPeerRPCMethod  = peerManageRPCMethod("ListPeer")
	listRouteRPCMethod = peerManageRPCMethod("ListRoute")
)

// ListPeer returns the peers visible to this EasyTier instance.
func (instance *Instance) ListPeer(
	ctx context.Context,
) (*apiinstance.ListPeerResponse, error) {
	response := new(apiinstance.ListPeerResponse)
	if err := instance.callRPC(
		ctx,
		listPeerRPCMethod,
		new(apiinstance.ListPeerRequest),
		response,
	); err != nil {
		return nil, err
	}
	return response, nil
}

// ListRoute returns the routing table visible to this EasyTier instance.
func (instance *Instance) ListRoute(
	ctx context.Context,
) (*apiinstance.ListRouteResponse, error) {
	response := new(apiinstance.ListRouteResponse)
	if err := instance.callRPC(
		ctx,
		listRouteRPCMethod,
		new(apiinstance.ListRouteRequest),
		response,
	); err != nil {
		return nil, err
	}
	return response, nil
}

func (instance *Instance) callRPC(
	ctx context.Context,
	method protoreflect.MethodDescriptor,
	request proto.Message,
	response proto.Message,
) error {
	if instance == nil || instance.engine == nil {
		return fmt.Errorf("call RPC through nil EasyTier instance")
	}
	if ctx == nil {
		return fmt.Errorf("call EasyTier RPC with nil context")
	}
	timeoutMillis, err := rpcTimeoutMillis(ctx)
	if err != nil {
		return err
	}
	requestPayload, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode EasyTier RPC %s request: %w", method.FullName(), err)
	}
	service := method.Parent().(protoreflect.ServiceDescriptor)
	// EasyTier's RPC generator uses the service name for both descriptor
	// names and numbers methods from one rather than protobuf's zero index.
	encodedRequest, err := proto.Marshal(&common.RpcRequest{
		Descriptor_: &common.RpcDescriptor{
			ProtoName:   string(service.Name()),
			ServiceName: string(service.Name()),
			MethodIndex: uint32(method.Index() + 1),
		},
		Request:   requestPayload,
		TimeoutMs: timeoutMillis,
	})
	if err != nil {
		return fmt.Errorf("encode EasyTier RPC envelope: %w", err)
	}
	encodedResponse, err := instance.engine.RPC(ctx, encodedRequest)
	if err != nil {
		return err
	}
	var envelope common.RpcResponse
	if err := proto.Unmarshal(encodedResponse, &envelope); err != nil {
		return fmt.Errorf("decode EasyTier RPC %s response: %w", method.FullName(), err)
	}
	if envelope.Error != nil {
		return fmt.Errorf(
			"EasyTier RPC %s failed: %s",
			method.FullName(),
			envelope.Error,
		)
	}
	if err := proto.Unmarshal(envelope.Response, response); err != nil {
		return fmt.Errorf(
			"decode EasyTier RPC %s payload: %w",
			method.FullName(),
			err,
		)
	}
	return nil
}

func peerManageRPCMethod(name protoreflect.Name) protoreflect.MethodDescriptor {
	service := apiinstance.File_api_instance_proto.Services().ByName("PeerManageRpc")
	if service == nil {
		panic("generated EasyTier proto has no PeerManageRpc service")
	}
	method := service.Methods().ByName(name)
	if method == nil {
		panic("generated EasyTier proto has no PeerManageRpc." + string(name))
	}
	return method
}

func rpcTimeoutMillis(ctx context.Context) (int32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	timeout := defaultRPCTimeout
	if deadline, exists := ctx.Deadline(); exists {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, context.DeadlineExceeded
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	millis := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		millis++
	}
	return int32(millis), nil
}
