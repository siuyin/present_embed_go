package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ns *server.Server
)

func init() {
	embedNATS()
}

func main() {
	procWorker()
	http.HandleFunc("/proc", procHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
	ns.Shutdown()
}

func procWorker() {
	wrkr := getWorker()

	wrkr.Consume(func(m jetstream.Msg) {
		fmt.Printf("procWorker: received: %s: ", m.Data())
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("processed:  %s\n", m.Data())
		m.Ack()
	})

}
func getWorker() jetstream.Consumer {
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		log.Fatal("procWorker: Connect: ", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("procWorker: new jetstream: ", err)
	}

	ctx := context.Background()
	procStrm, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "proc", Subjects: []string{"proc"},
		MaxAge: 72 * time.Hour,
	})
	if err != nil {
		js.DeleteStream(ctx, "proc")
		log.Fatal("procWorker: create stream: ", err)
	}

	wrkr, err := procStrm.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "wrkr", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second})
	if err != nil {
		procStrm.DeleteConsumer(ctx, "wrkr")
		log.Fatal("procWorker: create consumer: ", err)
	}
	return wrkr
}

func procHandler(w http.ResponseWriter, r *http.Request) {
	n := r.FormValue("n")
	id := r.FormValue("id")
	if n == "" || id == "" {
		return
	}

	nn, err := strconv.Atoi(n)
	if err != nil {
		return
	}

	js, err := getJetStream()
	if err != nil {
		return
	}

	ctx := context.Background()
	for i := 0; i < nn; i++ {
		if _, err := js.Publish(ctx, "proc", []byte(fmt.Sprintf("%s:%d", id, i))); err != nil {
			log.Println("procHandler: publish: ", err)
		}

	}

	io.WriteString(w, fmt.Sprintf("Request ID: %s: %d processing requests queued.\n", id, nn))

	js.Conn().Flush()
	js.Conn().Close()

}
func getJetStream() (jetstream.JetStream, error) {

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return js, nil
}
func embedNATS() {
	var err error
	opts := &server.Options{ServerName: "testServer", Trace: false, Debug: false, JetStream: true, Port: 4222, StoreDir: "/tmp/js"}
	ns, err = server.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}

	go ns.Start()
	//ns.ConfigureLogger()
	if !ns.ReadyForConnections(5 * time.Second) {
		log.Fatal("Could not connect to embedded NATS server")
	}
}
