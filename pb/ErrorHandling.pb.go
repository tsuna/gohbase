//*
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains protocol buffers that are used for error handling

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.4
// 	protoc        v5.28.3
// source: ErrorHandling.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// *
// Protobuf version of a java.lang.StackTraceElement
// so we can serialize exceptions.
type StackTraceElementMessage struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	DeclaringClass *string                `protobuf:"bytes,1,opt,name=declaring_class,json=declaringClass" json:"declaring_class,omitempty"`
	MethodName     *string                `protobuf:"bytes,2,opt,name=method_name,json=methodName" json:"method_name,omitempty"`
	FileName       *string                `protobuf:"bytes,3,opt,name=file_name,json=fileName" json:"file_name,omitempty"`
	LineNumber     *int32                 `protobuf:"varint,4,opt,name=line_number,json=lineNumber" json:"line_number,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *StackTraceElementMessage) Reset() {
	*x = StackTraceElementMessage{}
	mi := &file_ErrorHandling_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StackTraceElementMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StackTraceElementMessage) ProtoMessage() {}

func (x *StackTraceElementMessage) ProtoReflect() protoreflect.Message {
	mi := &file_ErrorHandling_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StackTraceElementMessage.ProtoReflect.Descriptor instead.
func (*StackTraceElementMessage) Descriptor() ([]byte, []int) {
	return file_ErrorHandling_proto_rawDescGZIP(), []int{0}
}

func (x *StackTraceElementMessage) GetDeclaringClass() string {
	if x != nil && x.DeclaringClass != nil {
		return *x.DeclaringClass
	}
	return ""
}

func (x *StackTraceElementMessage) GetMethodName() string {
	if x != nil && x.MethodName != nil {
		return *x.MethodName
	}
	return ""
}

func (x *StackTraceElementMessage) GetFileName() string {
	if x != nil && x.FileName != nil {
		return *x.FileName
	}
	return ""
}

func (x *StackTraceElementMessage) GetLineNumber() int32 {
	if x != nil && x.LineNumber != nil {
		return *x.LineNumber
	}
	return 0
}

// *
// Cause of a remote failure for a generic exception. Contains
// all the information for a generic exception as well as
// optional info about the error for generic info passing
// (which should be another protobuffed class).
type GenericExceptionMessage struct {
	state         protoimpl.MessageState      `protogen:"open.v1"`
	ClassName     *string                     `protobuf:"bytes,1,opt,name=class_name,json=className" json:"class_name,omitempty"`
	Message       *string                     `protobuf:"bytes,2,opt,name=message" json:"message,omitempty"`
	ErrorInfo     []byte                      `protobuf:"bytes,3,opt,name=error_info,json=errorInfo" json:"error_info,omitempty"`
	Trace         []*StackTraceElementMessage `protobuf:"bytes,4,rep,name=trace" json:"trace,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GenericExceptionMessage) Reset() {
	*x = GenericExceptionMessage{}
	mi := &file_ErrorHandling_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GenericExceptionMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenericExceptionMessage) ProtoMessage() {}

func (x *GenericExceptionMessage) ProtoReflect() protoreflect.Message {
	mi := &file_ErrorHandling_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenericExceptionMessage.ProtoReflect.Descriptor instead.
func (*GenericExceptionMessage) Descriptor() ([]byte, []int) {
	return file_ErrorHandling_proto_rawDescGZIP(), []int{1}
}

func (x *GenericExceptionMessage) GetClassName() string {
	if x != nil && x.ClassName != nil {
		return *x.ClassName
	}
	return ""
}

func (x *GenericExceptionMessage) GetMessage() string {
	if x != nil && x.Message != nil {
		return *x.Message
	}
	return ""
}

func (x *GenericExceptionMessage) GetErrorInfo() []byte {
	if x != nil {
		return x.ErrorInfo
	}
	return nil
}

func (x *GenericExceptionMessage) GetTrace() []*StackTraceElementMessage {
	if x != nil {
		return x.Trace
	}
	return nil
}

// *
// Exception sent across the wire when a remote task needs
// to notify other tasks that it failed and why
type ForeignExceptionMessage struct {
	state            protoimpl.MessageState   `protogen:"open.v1"`
	Source           *string                  `protobuf:"bytes,1,opt,name=source" json:"source,omitempty"`
	GenericException *GenericExceptionMessage `protobuf:"bytes,2,opt,name=generic_exception,json=genericException" json:"generic_exception,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *ForeignExceptionMessage) Reset() {
	*x = ForeignExceptionMessage{}
	mi := &file_ErrorHandling_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ForeignExceptionMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ForeignExceptionMessage) ProtoMessage() {}

func (x *ForeignExceptionMessage) ProtoReflect() protoreflect.Message {
	mi := &file_ErrorHandling_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ForeignExceptionMessage.ProtoReflect.Descriptor instead.
func (*ForeignExceptionMessage) Descriptor() ([]byte, []int) {
	return file_ErrorHandling_proto_rawDescGZIP(), []int{2}
}

func (x *ForeignExceptionMessage) GetSource() string {
	if x != nil && x.Source != nil {
		return *x.Source
	}
	return ""
}

func (x *ForeignExceptionMessage) GetGenericException() *GenericExceptionMessage {
	if x != nil {
		return x.GenericException
	}
	return nil
}

var File_ErrorHandling_proto protoreflect.FileDescriptor

var file_ErrorHandling_proto_rawDesc = string([]byte{
	0x0a, 0x13, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x48, 0x61, 0x6e, 0x64, 0x6c, 0x69, 0x6e, 0x67, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x70, 0x62, 0x22, 0xa2, 0x01, 0x0a, 0x18, 0x53, 0x74,
	0x61, 0x63, 0x6b, 0x54, 0x72, 0x61, 0x63, 0x65, 0x45, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x4d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x27, 0x0a, 0x0f, 0x64, 0x65, 0x63, 0x6c, 0x61, 0x72,
	0x69, 0x6e, 0x67, 0x5f, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0e, 0x64, 0x65, 0x63, 0x6c, 0x61, 0x72, 0x69, 0x6e, 0x67, 0x43, 0x6c, 0x61, 0x73, 0x73, 0x12,
	0x1f, 0x0a, 0x0b, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x1b, 0x0a, 0x09, 0x66, 0x69, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x66, 0x69, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a,
	0x0b, 0x6c, 0x69, 0x6e, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x0a, 0x6c, 0x69, 0x6e, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x22, 0xa5,
	0x01, 0x0a, 0x17, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c,
	0x61, 0x73, 0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x63, 0x6c, 0x61, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x5f, 0x69, 0x6e, 0x66,
	0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x49, 0x6e,
	0x66, 0x6f, 0x12, 0x32, 0x0a, 0x05, 0x74, 0x72, 0x61, 0x63, 0x65, 0x18, 0x04, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x1c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x74, 0x61, 0x63, 0x6b, 0x54, 0x72, 0x61, 0x63,
	0x65, 0x45, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x52,
	0x05, 0x74, 0x72, 0x61, 0x63, 0x65, 0x22, 0x7b, 0x0a, 0x17, 0x46, 0x6f, 0x72, 0x65, 0x69, 0x67,
	0x6e, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x48, 0x0a, 0x11, 0x67, 0x65, 0x6e,
	0x65, 0x72, 0x69, 0x63, 0x5f, 0x65, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x62, 0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69,
	0x63, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x52, 0x10, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x42, 0x4d, 0x0a, 0x2a, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68,
	0x65, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65,
	0x64, 0x42, 0x13, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x48, 0x61, 0x6e, 0x64, 0x6c, 0x69, 0x6e, 0x67,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x48, 0x01, 0x5a, 0x05, 0x2e, 0x2e, 0x2f, 0x70, 0x62, 0xa0,
	0x01, 0x01,
})

var (
	file_ErrorHandling_proto_rawDescOnce sync.Once
	file_ErrorHandling_proto_rawDescData []byte
)

func file_ErrorHandling_proto_rawDescGZIP() []byte {
	file_ErrorHandling_proto_rawDescOnce.Do(func() {
		file_ErrorHandling_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_ErrorHandling_proto_rawDesc), len(file_ErrorHandling_proto_rawDesc)))
	})
	return file_ErrorHandling_proto_rawDescData
}

var file_ErrorHandling_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_ErrorHandling_proto_goTypes = []any{
	(*StackTraceElementMessage)(nil), // 0: pb.StackTraceElementMessage
	(*GenericExceptionMessage)(nil),  // 1: pb.GenericExceptionMessage
	(*ForeignExceptionMessage)(nil),  // 2: pb.ForeignExceptionMessage
}
var file_ErrorHandling_proto_depIdxs = []int32{
	0, // 0: pb.GenericExceptionMessage.trace:type_name -> pb.StackTraceElementMessage
	1, // 1: pb.ForeignExceptionMessage.generic_exception:type_name -> pb.GenericExceptionMessage
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_ErrorHandling_proto_init() }
func file_ErrorHandling_proto_init() {
	if File_ErrorHandling_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_ErrorHandling_proto_rawDesc), len(file_ErrorHandling_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ErrorHandling_proto_goTypes,
		DependencyIndexes: file_ErrorHandling_proto_depIdxs,
		MessageInfos:      file_ErrorHandling_proto_msgTypes,
	}.Build()
	File_ErrorHandling_proto = out.File
	file_ErrorHandling_proto_goTypes = nil
	file_ErrorHandling_proto_depIdxs = nil
}
