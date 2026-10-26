package main

import (
	proto "chitchat/grpc"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
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
		defer func() {
			recover(); // make sure any panics are caught

			// send leave message (if joined)
			if name != "" {
				s.RWMutex.Lock()
				s.timestamp += 1
				log.Printf("[SERVER] Participant '%s' (id: %d) left Chit Chat at logical time %d\n", name, id, s.timestamp)
				s.messageChannel <- Message{
					message:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", name, s.timestamp),
					author_name: "Server",
					author_id:   0,
					timestamp:   s.timestamp,
				}
				s.RWMutex.Unlock()
			}
			close(vchan); // make outer goroutine close
		}()
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) || req == nil {
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
					log.Printf("[SERVER] Participant '%s' (id: %d) joined Chit Chat at logical time %d\n", name, id, s.timestamp)
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
					log.Printf("[SERVER] Got message from '%s' (id: %d) at logical time %d: %s\n", name, id, s.timestamp, *req.Message)
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
			select {
			case _, ok := <-vchan:
				if !ok {
					return
				}
			default:
				stream.Send(&proto.GetStreamResponse{
					Message:    &msg.message,
					AuthorName: &msg.author_name,
					AuthorId:   &msg.author_id,
					Timestamp:  msg.timestamp,
				})
			}
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
		log.Printf("[SERVER]\tserver is listening on rpc://localhost:3000\n\t\t\t\tPress Ctrl+C to exit.\n")
	}

	go func() {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	    defer stop()

	    // Block until the context is canceled
	    <-ctx.Done()

	    fmt.Println("\n[SERVER] Interrupt received, shutting down gracefully...")
		lis.Close()
	}()

	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServiceServer(grpcServer, newService())
	grpcServer.Serve(lis)
}
