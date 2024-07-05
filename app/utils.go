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

func eventLoop(queue chan func(), inspection chan []func()) {
	var pendingCommands []func()

	go func() {
		for {
			queue <- func() {}
			<-queue
		}
	}()

	for {
		select {
		case cmd := <-queue:
			if cmd != nil {
				pendingCommands = append(pendingCommands, cmd)
				go func(cmd func()) {
					cmd()
					pendingCommands = pendingCommands[1:]
				}(cmd)
			}
		case inspection <- pendingCommands:
		}
	}
}

func propagateCommand(command string, args []string, originConn net.Conn) {
	for port, server := range hosts {
		if server.conn != originConn && server.data["role"] == "slave" {
			err := sendCommand(server.conn, command, args)
			if err != nil {
				fmt.Printf("Failed to send command to replica on port %s: %v\n", port, err)
			}
		}
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

func byteBulkStringLen(command string, args []string) int {
	cmdArray := fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(command), command)
	for _, arg := range args {
		cmdArray += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return len([]byte(cmdArray))
}

func printHosts() {
	fmt.Println("Current state of hosts:")
	for port, server := range hosts {
		fmt.Printf("Port: %s\n", port)
		fmt.Printf("Role: %s\n", server.data["role"])
		fmt.Printf("Stash: %s\n", server.stash)
		fmt.Printf("Master ReplID: %s\n", server.data["master_replid"])
		fmt.Printf("Master Repl Offset: %s\n", server.data["master_repl_offset"])
		fmt.Printf("Processed Bytes: %d\n", server.processedBytes)
		fmt.Println()
	}
}
