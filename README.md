# ChitChat

Please see the two sections below on how to run the server and the client.

The server uses port 3000, so make sure nothing uses it as well.

You must have downloaded the repository and have a terminal for each server/client open, with the
working directory being the git root.

## Server

To run the server, make sure to have golang, gRPC, and protobuf installed.
It is recommended have Make (GNU) installed, but not necessary.

To run the server (with Make installed), run the following commands in your terminal:
```bash
$ make grpc
$ go run server/*.go
```
Without Make, you must run the following commands in your terminal:
```bash
$ protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/chitchat.proto
$ go run server/*.go
```

## Client

To run the client, make sure the server is running.
And then run the following command in your terminal:
```bash
$ go run client/*.go
```

The client will then ask for your (user)name for identification.
Put it in and press enter.

Afterwards you can write messages and send them by pressing enter. (Multi line messages is NOT supported)

To leave ChitChat, write an empty line.
