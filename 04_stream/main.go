package main

import (
	"context"
	"embedgo/03_embed/public"
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
	http.HandleFunc("/proc", procHandler)               // API team
	http.HandleFunc("/stor", storHandler)               // API team
	http.Handle("/", http.FileServerFS(public.Content)) // FrontEnd team

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
	js, err := getJetStream()
	if err != nil {
		log.Fatal("getWorker: getJetStream: ", err)
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
		Durable: "wrkr", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 3 * time.Second})
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
	defer jetStreamClose(js)

	ctx := context.Background()
	for i := 0; i < nn; i++ {
		tm := time.Now().Format("04:05.0000000")
		if _, err := js.Publish(ctx, "proc", []byte(fmt.Sprintf("%s: %s:%d", tm, id, i))); err != nil {
			log.Println("procHandler: publish: ", err)
		}
	}

	io.WriteString(w, fmt.Sprintf("Request ID: %s: %d processing requests queued.\n", id, nn))
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

func storHandler(w http.ResponseWriter, r *http.Request) {
	js, err := getJetStream()
	if err != nil {
		log.Println("storHandler: getJetStream: ", err)
	}
	defer jetStreamClose(js)

	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "myKV"})
	if err != nil {
		log.Println("storHandler: create KV: ", err)
	}

	switch r.Method {
	case "GET":
		k := r.FormValue("k")
		v, err := kv.Get(ctx, k)
		if err != nil {
			fmt.Fprintf(w, "get: %s: %v\n", k, err)
			return
		}

		fmt.Fprintf(w, "key: %s, value: %s\n", k, v.Value())
	case "POST":
		k := r.FormValue("k")
		v := r.FormValue("v")
		_, err := kv.Put(ctx, k, []byte(v))
		if err != nil {
			fmt.Fprintf(w, "put: %s: %s: %v\n", k, v, err)
			return
		}

		fmt.Fprintf(w, "put %s into key: %s\n", v, k)
	default:
		fmt.Fprintf(w, "%s: handling not implemented\n", r.Method)
	}

}

func jetStreamClose(js jetstream.JetStream) {
	js.Conn().Flush()
	js.Conn().Close()
}
