package main

import (
	"fmt"
	"net"
	"strconv"
)

func sendPing(conn net.Conn) error {
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		return fmt.Errorf("failed to send PING: %v", err)
	}

	response, err := readResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read PING response: %v", err)
	}

	fmt.Println("Received PING response:", response)
	return nil
}

func sendReplConf(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$" + strconv.Itoa(len(args[0])) + "\r\n" + args[0] + "\r\n$" + strconv.Itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"))
	if err != nil {
		return fmt.Errorf("failed to send REPLCONF: %v", err)
	}
	_, err = readResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read PING response: %v", err)
	}
	return nil
}

func sendPsync(conn net.Conn, args []string) error {
	//_, err := conn.Write([]byte("*3\r\n$4\r\nPSYNC\r\n$" + strconv.Itoa(len(args[0])) + "\r\n" + args[0] + "\r\n$" + strconv.Itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"))
	_, err := conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		return fmt.Errorf("failed to send PSYNC: %v", err)
	}
	response, err := readResponse(conn)
	if err != nil {
		return fmt.Errorf("failed to read PING response: %v", err)
	}
	fmt.Println("Received PSYNC response:", response)
	return nil
}

func sendError(conn net.Conn, msg string) {
	conn.Write([]byte("-ERR " + msg + "\r\n"))
}
