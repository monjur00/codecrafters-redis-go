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

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type resp struct {
	cmd  string
	args []string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handlerConn(con)
	}
}

func handlerConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		r, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading connection: ", err.Error())
		}

		fmt.Printf("Received the following %+v\n", r)

		switch r.cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(r.args) > 0 {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(r.args[0]), r.args[0])
			}
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}

	}
}

func parseRESP(r *bufio.Reader) (*resp, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")

	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got:%s", line)
	}

	arrLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line[1:])
	}

	if arrLen == 0 {
		return nil, fmt.Errorf("empty array")
	}

	args := make([]string, arrLen)
	for i := range arrLen {
		arg, err := parseBulkString(r)
		if err != nil {
			return nil, err
		}
		args[i] = arg
	}

	return &resp{cmd: args[0], args: args[1:]}, nil
}

func parseBulkString(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")

	if !strings.HasPrefix(line, "$") {
		return "", fmt.Errorf("expected bulk string, got:%s", line)
	}

	strLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %s", line[1:])
	}

	if strLen < 0 {
		return "", nil // null bulk string, this will not skip the buffer reader lst seek position
	}

	// Read the actual string content
	content := make([]byte, strLen+2) // +2 for reading \r\n
	_, err = r.Read(content)
	if err != nil {
		return "", err
	}

	// Remove the trailing \r\n
	return string(content[:strLen]), nil
}
