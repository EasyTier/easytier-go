package engine

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/EasyTier/easytier-go-host/internal/coreabi"
)

type recordingCore struct {
	calls []string
}

func (*recordingCore) Start(context.Context) error { return nil }
func (*recordingCore) Stop(context.Context) error  { return nil }
func (core *recordingCore) Drive(context.Context) (coreabi.State, error) {
	core.calls = append(core.calls, "drive")
	return coreabi.StateRunning, nil
}
func (core *recordingCore) NotifyCompletions(context.Context) error {
	core.calls = append(core.calls, "notify")
	return nil
}
func (core *recordingCore) NextDeadline(context.Context) (int64, error) {
	core.calls = append(core.calls, "deadline")
	return math.MaxInt64, nil
}
func (core *recordingCore) SendPacket(context.Context, []byte) error {
	core.calls = append(core.calls, "send")
	return nil
}
func (*recordingCore) Drop(context.Context) error { return nil }

func TestCompletionNotifiesGuestBeforeDriving(t *testing.T) {
	core := &recordingCore{}
	instance := &Instance{
		host:    &Host{},
		ctx:     context.Background(),
		core:    core,
		running: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	if _, err := instance.drive(true); err != nil {
		t.Fatalf("drive completion: %v", err)
	}
	want := []string{"notify", "drive", "deadline"}
	if !reflect.DeepEqual(core.calls, want) {
		t.Fatalf("completion call order = %v, want %v", core.calls, want)
	}
}

func TestPacketIngressBatchDrivesOnce(t *testing.T) {
	core := &recordingCore{}
	instance := &Instance{
		host:     &Host{},
		ctx:      context.Background(),
		core:     core,
		commands: make(chan command, maximumPacketIngressBatch),
		running:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	requests := make([]command, 3)
	for index := range requests {
		requests[index] = command{
			kind:     commandSendPacket,
			packet:   []byte{byte(index)},
			response: make(chan error, 1),
		}
		if index != 0 {
			instance.commands <- requests[index]
		}
	}

	if _, err := instance.handlePacketBatch(requests[0], math.MaxInt64); err != nil {
		t.Fatalf("handle packet batch: %v", err)
	}
	for index := range requests {
		if err := <-requests[index].response; err != nil {
			t.Fatalf("packet %d response: %v", index, err)
		}
	}
	want := []string{"send", "send", "send", "drive", "deadline"}
	if !reflect.DeepEqual(core.calls, want) {
		t.Fatalf("packet batch call order = %v, want %v", core.calls, want)
	}
}
