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
	if hosts[port].data["role"] == "master" {
		propagateCommand("SET", args, conn)
	}
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
	if len(args) == 2 && args[0] == "listening-port" {
		port := args[1]
		if _, exists := hosts[port]; !exists {
			hosts[port] = redisServer{conn, map[string]string{
				"role":               "slave",
				"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				"master_repl_offset": "0",
			}}
			fmt.Printf("Added new host with listening-port %s\n", port)
		}
	}
	conn.Write([]byte("+OK\r\n"))
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

	_, err = conn.Write([]byte("$" + strconv.Itoa(len(rdbBytes)) + "\r\n" + string(rdbBytes)))
	if err != nil {
		return fmt.Errorf("failed to send RDB length: %v", err)
	}
	if err != nil {
		return fmt.Errorf("failed to send RDB contents: %v", err)
	}
	return nil
}
