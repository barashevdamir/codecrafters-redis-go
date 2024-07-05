package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func registerCommands() {
	commands["PING"] = Command{Handler: handlePing}
	commands["ECHO"] = Command{Handler: handleEcho}
	commands["SET"] = Command{Handler: handleSet}
	commands["GET"] = Command{Handler: handleGet}
	commands["INFO"] = Command{Handler: handleInfo}
	commands["REPLCONF"] = Command{Handler: handleReplConf}
	commands["PSYNC"] = Command{Handler: handlePsync}
}

func handlePing(conn net.Conn, args []string) {
	server, _ := hosts[port]
	fmt.Println("handlePing called with args:", args)
	if server.data["role"] == "slave" {
		server.processedBytes += byteBulkStringLen("PING", args)
		fmt.Println("Updated processedBytes for PING:", server.processedBytes)
	}
	fmt.Printf("Adding to offset %d after %s\n", byteBulkStringLen("PING", args), "PING")
	offset += byteBulkStringLen("PING", args)
	_, err := conn.Write([]byte("+PONG\r\n")) // Отправляем ответ на PING
	if err != nil {
		fmt.Println("Error sending PONG:", err)
	}
}

func handleEcho(conn net.Conn, args []string) {
	server, _ := hosts[port]
	if len(args) > 0 {
		if server.data["role"] == "slave" {
			server.processedBytes += byteBulkStringLen("ECHO", args)
			return
		}
		fmt.Printf("Adding to offset %d after %s\n", byteBulkStringLen("ECHO", args), "ECHO")
		offset += byteBulkStringLen("ECHO", args)
		conn.Write([]byte("$" + strconv.Itoa(len(args[0])) + "\r\n" + args[0] + "\r\n"))
	} else {
		sendError(conn, "no message")
	}
}

func handleSet(conn net.Conn, args []string) {
	server, _ := hosts[port]

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
	server.stash[key] = value
	printHosts()
	if expiration > 0 {
		time.AfterFunc(time.Duration(expiration)*time.Millisecond, func() {
			delete(server.stash, key)
		})
	}
	fmt.Printf("Adding to offset %d after %s\n", byteBulkStringLen("SET", args), "SET")
	offset += byteBulkStringLen("SET", args)
	if server.data["role"] == "slave" {
		server.processedBytes += byteBulkStringLen("SET", args)
		return
	}
	conn.Write([]byte("+OK\r\n"))
	if server.data["role"] == "master" {

		propagateCommand("SET", args, conn)
	}
}

func handleGet(conn net.Conn, args []string) {
	server, _ := hosts[port]
	printHosts()
	if len(args) > 0 {
		_, ok := server.stash[args[0]]
		if ok {
			conn.Write([]byte("$" + strconv.Itoa(len(server.stash[args[0]])) + "\r\n" + server.stash[args[0]] + "\r\n"))
		} else {
			conn.Write([]byte("$-1\r\n"))
		}
	}
	fmt.Printf("Adding to offset %d after %s\n", byteBulkStringLen("GET", args), "GET")
	offset += byteBulkStringLen("GET", args)
	if server.data["role"] == "slave" {
		server.processedBytes += byteBulkStringLen("GET", args)
		return
	} else {
		sendError(conn, "no message")
	}
}

func handleInfo(conn net.Conn, args []string) {
	server, _ := hosts[port]

	var dataStr string
	_data := server.data
	for key, val := range _data {
		dataStr += "$" + strconv.Itoa(len(key+val)) + "\r\n" + key + ":" + val + "\r\n"
	}

	conn.Write([]byte("$" + strconv.Itoa(len(dataStr)) + "\r\n" + dataStr + "\r\n"))
	fmt.Printf("Adding to offset %d after %s\n", byteBulkStringLen("INFO", args), "INFO")
	offset += byteBulkStringLen("INFO", args)
	if server.data["role"] == "slave" {
		server.processedBytes += byteBulkStringLen("INFO", args)
		return
	}
}

func handleReplConf(conn net.Conn, args []string) {
	fmt.Println("handleReplConf called with args:", args)
	if len(args) == 2 && args[0] == "listening-port" {
		port := args[1]
		if _, exists := hosts[port]; !exists {
			hosts[port] = &redisServer{
				conn: conn,
				data: map[string]string{
					"role":               "slave",
					"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
					"master_repl_offset": "0",
				},
				stash:          map[string]string{},
				processedBytes: 0,
			}
			fmt.Printf("Added new host with listening-port %s\n", port)
		}
	}
	if args[0] == "listening-port" || args[0] == "capa" {
		conn.Write([]byte("+OK\r\n"))
	}
	if args[0] == "GETACK" {
		printHosts()
		server, _ := hosts[port]
		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.Itoa(offset)), offset)
		fmt.Println("Sending ACK response:", response)
		conn.Write([]byte(response))
		server.processedBytes = 0
	}
}

func handlePsync(conn net.Conn, args []string) {
	conn.Write([]byte("+FULLRESYNC " + hosts[port].data["master_replid"] + " " + hosts[port].data["master_repl_offset"] + "\r\n"))
	sendRDB(conn)
}

func sendRDB(conn net.Conn) error {
	const emptyRDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	rdbBytes, err := hex.DecodeString(emptyRDB)
	if err != nil {
		rdbBytes, err = base64.StdEncoding.DecodeString(emptyRDB)
		if err != nil {
			return fmt.Errorf("failed to decode empty RDB: %v", err)
		}
	}

	_, err = conn.Write([]byte("$" + strconv.Itoa(len(rdbBytes)) + "\r\n"))
	if err != nil {
		return fmt.Errorf("failed to send RDB content: %v", err)
	}

	return nil
}
