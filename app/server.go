package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// CommandFunc тип функции для обработки команды
type CommandFunc func(conn net.Conn, args []string)

// Command структура, описывающая команду
type Command struct {
	Handler CommandFunc
}

var commands = map[string]Command{}

func main() {
	registerCommands()

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
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, queue)
	}
}

func registerCommands() {
	commands["PING"] = Command{Handler: handlePing}
	commands["ECHO"] = Command{Handler: handleEcho}
}

func handleConnection(conn net.Conn, queue chan func()) {
	reader := bufio.NewReader(conn)
	defer conn.Close()

	for {
		dataType, err := reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading:", err.Error())
			}
			break
		}

		if dataType == '*' {
			handleArray(reader, conn, queue)
		} else {
			sendError(conn, "invalid data type")
			continue
		}
	}
}

func handleArray(reader *bufio.Reader, conn net.Conn, queue chan func()) {
	sizeStr, err := reader.ReadString('\n')
	if err != nil {
		sendError(conn, "bad array size")
		return
	}

	size, err := strconv.Atoi(strings.TrimSpace(sizeStr))
	if err != nil || size < 1 {
		sendError(conn, "bad array size")
		return
	}

	_, _ = reader.ReadString('\n')
	command, _ := reader.ReadString('\n')
	command = strings.TrimSpace(command)

	var message string
	if command == "ECHO" {
		_, _ = reader.ReadString('\n')
		message, _ = reader.ReadString('\n')
		message = strings.TrimSpace(message)
	}

	cmd, ok := commands[strings.ToUpper(command)]
	if !ok {
		sendError(conn, "unknown command")
		return
	}

	cmd.Handler(conn, []string{message})
}

func handlePing(conn net.Conn, args []string) {
	conn.Write([]byte("+PONG\r\n"))
}

func handleEcho(conn net.Conn, args []string) {
	if len(args) > 0 {
		conn.Write([]byte("$" + strconv.Itoa(len(args[0])) + "\r\n" + args[0] + "\r\n"))
	} else {
		sendError(conn, "no message")
	}
}

func sendError(conn net.Conn, msg string) {
	conn.Write([]byte("-ERR " + msg + "\r\n"))
}

func eventLoop(queue chan func()) {
	for {
		queue <- func() {}
		<-queue
	}
}
