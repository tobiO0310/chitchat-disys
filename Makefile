install-grpc:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

grpc/chitchat.pb.go: grpc/chitchat.proto
	protoc --go_out=. --go_opt=paths=source_relative grpc/chitchat.proto
grpc/chitchat_grpc.pb.go: grpc/chitchat.proto
	protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/chitchat.proto

grpc: grpc/chitchat.pb.go grpc/chitchat_grpc.pb.go