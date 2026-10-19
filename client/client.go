package main

import (
	"bufio"
	proto "chitchat/grpc"
	"context"
	"io"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Message struct {
	message     string
	author_name string
	author_id   uint64
	timestamp   uint64
}

func main() {
	conn, err := grpc.NewClient("localhost:3000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[CLIENT] Failed to connect to Server")
	}
	defer conn.Close()

	var messages []Message

	client := proto.NewChitChatServiceClient(conn)

	stream, err := client.GetStream(context.Background())

	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("[CLIENT] Failed to receive a note : %v", err)
			}
			messages = append(messages, Message{
				message:     *in.Message,
				author_name: *in.AuthorName,
				author_id:   *in.AuthorId,
				timestamp:   in.Timestamp,
			})
			log.Printf("[CLIENT] Got message from '%s' (id: %d) at logical time %d: %s\n", *in.AuthorName, *in.AuthorId, in.Timestamp, *in.Message)
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	log.Print("[CLIENT] Input your name: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)
	log.Printf("[CLIENT] Joining as '%s'", name)
	stream.Send(&proto.GetStreamRequest{
		Type:    proto.MessageType_MESSAGE_TYPE_JOIN,
		Message: &name,
	})

	log.Println("[CLIENT] Send messages on each line from now on (empty message = quit)")
	for {
		msg, _ := reader.ReadString('\n')
		msg = strings.TrimSpace(msg)
		if msg == "" {
			break
		}
		stream.Send(&proto.GetStreamRequest{
			Type:    proto.MessageType_MESSAGE_TYPE_MESSAGE,
			Message: &msg,
		})
	}

	stream.Send(&proto.GetStreamRequest{
		Type: proto.MessageType_MESSAGE_TYPE_LEAVE,
	})

	stream.CloseSend()
	<-waitc
}
