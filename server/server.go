package main

import (
	proto "chitchat/grpc"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type ChitChatService struct {
	proto.UnimplementedChitChatServiceServer

	sync.RWMutex
	timestamp    uint64
	lastClientId uint64
	Piper
	messageChannel chan Message
}

func (s *ChitChatService) GetStream(stream grpc.BidiStreamingServer[proto.GetStreamRequest, proto.GetStreamResponse]) error {
	s.RWMutex.Lock()
	id := s.lastClientId + 1
	s.lastClientId += 1
	s.RWMutex.Unlock()
	name := ""

	vchan := make(chan struct{})

	go func() {
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}

			switch req.Type {
			case proto.MessageType_MESSAGE_TYPE_JOIN:
				{
					if name != "" || req.Message == nil || *req.Message == "" {
						break
					}
					name = *req.Message
					s.RWMutex.Lock()
					s.timestamp += 1
					s.messageChannel <- Message{
						message:     fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", name, s.timestamp),
						author_name: "Server",
						author_id:   0,
						timestamp:   s.timestamp,
					}
					s.RWMutex.Unlock()
					break
				}
			case proto.MessageType_MESSAGE_TYPE_MESSAGE:
				{
					if name == "" || req.Message == nil || *req.Message == "" {
						break
					}
					s.RWMutex.Lock()
					s.timestamp += 1
					s.messageChannel <- Message{
						message:     *req.Message,
						author_name: name,
						author_id:   id,
						timestamp:   s.timestamp,
					}
					s.RWMutex.Unlock()
					break
				}
			case proto.MessageType_MESSAGE_TYPE_LEAVE:
				{
					defer close(vchan)
					if name == "" {
						return
					}
					s.RWMutex.Lock()
					s.timestamp += 1
					s.messageChannel <- Message{
						message:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", name, s.timestamp),
						author_name: "Server",
						author_id:   0,
						timestamp:   s.timestamp,
					}
					s.RWMutex.Unlock()
					return
				}

			default:
			case proto.MessageType_MESSAGE_TYPE_UNSPECIFIED:
			}
		}
	}()

	go func() {
		c := s.Piper.Pipe()

		for msg := range c {
			stream.Send(&proto.GetStreamResponse{
				Message:    &msg.message,
				AuthorName: &msg.author_name,
				AuthorId:   &msg.author_id,
				Timestamp:  msg.timestamp,
			})
		}
	}()

	<-vchan
	// function that reads messages (there are many receivers, one -> many)

	return nil
}

func newService() *ChitChatService {
	c := make(chan Message)
	p := NewPiper(c)
	return &ChitChatService{
		Piper:          p,
		messageChannel: c,
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:3000")
	if err != nil {
		log.Fatalf("[SERVER] failed to listen: %v\n", err)
	} else {
		log.Printf("[SERVER] server is listening on rpc://localhost:3000\n")
	}
	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServiceServer(grpcServer, newService())
	grpcServer.Serve(lis)
}
