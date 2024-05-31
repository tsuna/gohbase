// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/tsuna/gohbase (interfaces: RPCClient)
//
// Generated by this command:
//
//	mockgen -destination=rpcclient.go -package=mock github.com/tsuna/gohbase RPCClient
//

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	hrpc "github.com/tsuna/gohbase/hrpc"
	gomock "go.uber.org/mock/gomock"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// MockRPCClient is a mock of RPCClient interface.
type MockRPCClient struct {
	ctrl     *gomock.Controller
	recorder *MockRPCClientMockRecorder
}

// MockRPCClientMockRecorder is the mock recorder for MockRPCClient.
type MockRPCClientMockRecorder struct {
	mock *MockRPCClient
}

// NewMockRPCClient creates a new mock instance.
func NewMockRPCClient(ctrl *gomock.Controller) *MockRPCClient {
	mock := &MockRPCClient{ctrl: ctrl}
	mock.recorder = &MockRPCClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRPCClient) EXPECT() *MockRPCClientMockRecorder {
	return m.recorder
}

// SendRPC mocks base method.
func (m *MockRPCClient) SendRPC(arg0 hrpc.Call) (protoreflect.ProtoMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendRPC", arg0)
	ret0, _ := ret[0].(protoreflect.ProtoMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendRPC indicates an expected call of SendRPC.
func (mr *MockRPCClientMockRecorder) SendRPC(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendRPC", reflect.TypeOf((*MockRPCClient)(nil).SendRPC), arg0)
}
