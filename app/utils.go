package main

import (
	"bufio"
	"fmt"
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

func propagateCommand(command string, args []string, conn net.Conn) {
	for port, server := range hosts {
		if server.conn != conn {
			//if server.data["role"] == "slave" {
			err := sendCommand(server.conn, command, args)
			if err != nil {
				fmt.Printf("Failed to send command to replica on port %s: %v\n", port, err)
			}
		}
		//}
	}
}

func sendCommand(conn net.Conn, command string, args []string) error {

	cmdArray := fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(command), command)
	for _, arg := range args {
		cmdArray += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	_, err := conn.Write([]byte(cmdArray))
	if err != nil {
		return fmt.Errorf("failed to send command: %v", err)
	}
	return nil
}
