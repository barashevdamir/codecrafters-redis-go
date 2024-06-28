package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// CommandFunc тип функции для обработки команды
type CommandFunc func(conn net.Conn, args []string)

// Command структура, описывающая команду
type Command struct {
	Handler CommandFunc
}

type redisServer struct {
	conn net.Conn
	data map[string]string
}

var hosts = map[string]redisServer{}

var commands = map[string]Command{}
var stash = map[string]string{}
var (
	port      string
	replicaOf string
)

func main() {
	registerCommands()

	queue := make(chan func())
	go eventLoop(queue)

	flag.StringVar(&port, "port", "6379", "port to listen on")
	flag.StringVar(&replicaOf, "replicaof", "", "replica server")
	flag.Parse()

	if replicaOf != "" {
		replicaHostPort := strings.Split(replicaOf, " ")
		if len(replicaHostPort) == 2 {
			replicaHost := replicaHostPort[0]
			replicaPort := replicaHostPort[1]
			if _, exists := hosts[replicaPort]; !exists {
				fmt.Println("Replica host not found in hosts, attempting to connect to master.")
				go func() {
					err := performHandshake(replicaHost, replicaPort)
					if err != nil {
						fmt.Println("Failed to perform handshake with master:", err.Error())
						os.Exit(1)
					}
				}()
			}
		}
	}

	err := createServer(port, replicaOf, queue)
	if err != nil {
		fmt.Println("Failed to create server:", err.Error())
		os.Exit(1)
	}
}

func createServer(port, replicaOf string, queue chan func()) error {
	if _, exists := hosts[port]; exists {
		fmt.Printf("Server on port %s already exists.\n", port)
		return nil
	}

	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", port, err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %v", err)
		}
		fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr())
		if replicaOf != "" {
			hosts[port] = redisServer{conn, map[string]string{
				"role":               "slave",
				"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				"master_repl_offset": "0",
			}}
			masterHost, masterPort := strings.Split(replicaOf, " ")[0], strings.Split(replicaOf, " ")[1]
			go func() {
				err = performHandshake(masterHost, masterPort)
				if err != nil {
					fmt.Println("Error during handshake:", err.Error())
					return
				}
				fmt.Println("Handshake successful, continuing to accept requests.")
			}()
		} else {
			hosts[port] = redisServer{conn, map[string]string{
				"role":               "master",
				"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				"master_repl_offset": "0",
			}}
		}
		go handleConnection(conn, queue)
	}
}

func registerCommands() {
	commands["PING"] = Command{Handler: handlePing}
	commands["ECHO"] = Command{Handler: handleEcho}
	commands["SET"] = Command{Handler: handleSet}
	commands["GET"] = Command{Handler: handleGet}
	commands["INFO"] = Command{Handler: handleInfo}
	commands["REPLCONF"] = Command{Handler: handleReplConf}
}

func handleConnection(conn net.Conn, queue chan func()) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

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

	// Чтение команды
	_, _ = reader.ReadString('\n')
	command, _ := reader.ReadString('\n')
	command = strings.TrimSpace(command)
	// Чтение всех аргументов команды
	var args []string
	for i := 0; i < size-1; i++ {
		_, _ = reader.ReadString('\n')
		arg, _ := reader.ReadString('\n')
		arg = strings.TrimSpace(arg)
		args = append(args, arg)
	}

	cmd, ok := commands[strings.ToUpper(command)]
	if !ok {
		sendError(conn, "unknown command")
		return
	}

	cmd.Handler(conn, args)
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

func handleSet(conn net.Conn, args []string) {
	if len(args) < 2 {
		sendError(conn, "usage: SET key value [PX milliseconds]")
		return
	}

	key, value := args[0], args[1]
	expiration := 0

	if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
		var err error
		expiration, err = strconv.Atoi(args[3])
		if err != nil {
			sendError(conn, "invalid expiration time")
			return
		}
	}

	stash[key] = value

	if expiration > 0 {
		time.AfterFunc(time.Duration(expiration)*time.Millisecond, func() {
			delete(stash, key)
		})
	}

	conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, args []string) {
	if len(args) > 0 {
		_, ok := stash[args[0]]
		if ok {
			conn.Write([]byte("$" + strconv.Itoa(len(stash[args[0]])) + "\r\n" + stash[args[0]] + "\r\n"))
		} else {
			conn.Write([]byte("$-1\r\n"))
		}
	} else {
		sendError(conn, "no message")
	}
}

func handleInfo(conn net.Conn, args []string) {
	var dataStr string
	_data := hosts[port].data
	for key, val := range _data {
		dataStr += "$" + strconv.Itoa(len(key+val)) + "\r\n" + key + ":" + val + "\r\n"
	}
	conn.Write([]byte("$" + strconv.Itoa(len(dataStr)) + "\r\n" + dataStr + "\r\n"))
}

func handleReplConf(conn net.Conn, args []string) {
	conn.Write([]byte("+OK\r\n"))
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

func performHandshake(masterHost, masterPort string) error {
	address := net.JoinHostPort(masterHost, masterPort)
	fmt.Println("Connecting to master:", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	err = sendPing(conn)
	if err != nil {
		return err
	}
	err = sendReplConf(conn, []string{"listening-port", port})
	if err != nil {
		return err
	}
	err = sendReplConf(conn, []string{"capa", "psync2"})
	if err != nil {
		return err
	}

	go func() {
		reader := bufio.NewReader(conn)
		for {
			data, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Println("Error reading from master:", err.Error())
				}
				break
			}
			fmt.Println("Received from master:", data)
		}
	}()

	return nil
}

func sendPing(conn net.Conn) error {
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		return fmt.Errorf("failed to send PING: %v", err)
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
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
	return nil
}
