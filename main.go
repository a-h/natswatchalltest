package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewInProcessNATSServer() (conn *natsclient.Conn, js jetstream.JetStream, cleanup func(), err error) {
	tmp, err := os.MkdirTemp("", "nats_test")
	if err != nil {
		err = fmt.Errorf("failed to create temp directory for NATS storage: %w", err)
		return
	}
	server, err := natsserver.NewServer(&natsserver.Options{
		//ServerName: "monolith",
		DontListen: true, // Don't make a TCP socket.
		JetStream:  true,
		StoreDir:   tmp,
	})
	if err != nil {
		err = fmt.Errorf("failed to create NATS server: %w", err)
		return
	}
	// Add logs to stdout.
	// server.ConfigureLogger()
	server.Start()
	cleanup = func() {
		server.Shutdown()
		os.RemoveAll(tmp)
	}

	if !server.ReadyForConnections(time.Second * 5) {
		err = errors.New("failed to start server after 5 seconds")
		return
	}

	// Create a connection.
	conn, err = natsclient.Connect("", natsclient.InProcessServer(server))
	if err != nil {
		err = fmt.Errorf("failed to connect to server: %w", err)
		return
	}

	// Create a JetStream client.
	js, err = jetstream.New(conn)
	if err != nil {
		err = fmt.Errorf("failed to create jetstream: %w", err)
		return
	}

	return
}

func main() {
	// Arrange.
	_, js, shutdown, err := NewInProcessNATSServer()
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer shutdown()
	ctx := context.Background()

	// Create key/value bucket in NATS.
	bucketName := "test_kv"
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  bucketName,
		Storage: jetstream.MemoryStorage,
		History: 10, // Store previous 10 versions.
	})
	if err != nil {
		log.Fatalf("unexpected failure creating bucket: %v", err)
	}

	// Add 5 items to table.
	_, err = kv.Put(ctx, "user1", []byte("value: a"))
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}

	// If I comment this update out, I get the expected results...
	// But if I leave it in, the iteration stops at user 4.
	_, err = kv.Update(ctx, "user1", []byte("value: a"), 1)
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}

	_, err = kv.Put(ctx, "user2", []byte("value: b"))
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}
	_, err = kv.Put(ctx, "user3", []byte("value: c"))
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}
	_, err = kv.Put(ctx, "user4", []byte("value: d"))
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}
	_, err = kv.Put(ctx, "user5", []byte("value: e"))
	if err != nil {
		log.Fatalf("failed to create user5: %v", err)
	}

	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		log.Fatalf("failed to start watcher: %v", err)
	}

	for update := range watcher.Updates() {
		if update == nil {
			break
		}
		log.Printf("Got %s\n", update.Key())
	}
	watcher.Stop()
}
