package main

import (
	"embedgo/03_embed/public"
	"io"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/hello", helloHandler) // API team

	//http.Handle("/", http.FileServer(http.Dir("./public"))) // FrontEnd team
	http.Handle("/", http.FileServerFS(public.Content)) // FrontEnd team
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Hello World!")
}
