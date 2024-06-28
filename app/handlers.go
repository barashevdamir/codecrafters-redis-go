package main

import (
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

func handlePsync(conn net.Conn, args []string) {
	conn.Write([]byte("+FULLRESYNC " + hosts[port].data["master_replid"] + " " + hosts[port].data["master_repl_offset"] + "\r\n"))
}
