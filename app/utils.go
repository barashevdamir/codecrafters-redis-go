package main

import (
	"bufio"
	"net"
)

func readResponse(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return response, nil
}

func eventLoop(queue chan func()) {
	for {
		queue <- func() {}
		<-queue
	}
}
