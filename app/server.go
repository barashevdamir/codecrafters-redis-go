package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {

	queue := make(chan func())
	go eventLoop(queue)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, queue)
	}
}

func handleConnection(conn net.Conn, queue chan func()) {
	buf := make([]byte, 1024)
	defer conn.Close()
	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading:", err.Error())
			}
			break
		}
		queue <- func() {
			conn.Write([]byte("+PONG\r\n"))
		}

	}
}

func eventLoop(queue chan func()) {
	for task := range queue {
		task()
	}
}
