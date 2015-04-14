package text

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	gomem "github.com/dustin/gomemcached"
)

func str2op(cmd string, quiet bool) (cc gomem.CommandCode) {
	cc = 0xff
	if !quiet {
		switch cmd {
		case "set":
			cc = gomem.SET
		case "get", "gets":
			cc = gomem.GET
		case "add":
			cc = gomem.ADD
		case "replace":
			cc = gomem.REPLACE
		case "delete":
			cc = gomem.DELETE
		case "incr":
			cc = gomem.INCREMENT
		case "decr":
			cc = gomem.DECREMENT
		case "quit":
			cc = gomem.QUIT
		case "flush_all":
			cc = gomem.FLUSH
		case "noop":
			cc = gomem.NOOP
		case "append":
			cc = gomem.APPEND
		case "prepend":
			cc = gomem.PREPEND
		case "version":
			cc = gomem.VERSION
		}
	} else {
		switch cmd {
		case "set":
			cc = gomem.SETQ
		case "get", "gets":
			cc = gomem.GETQ
		case "add":
			cc = gomem.ADDQ
		case "replace":
			cc = gomem.REPLACEQ
		case "delete":
			cc = gomem.DELETEQ
		case "incr":
			cc = gomem.INCREMENTQ
		case "decr":
			cc = gomem.DECREMENTQ
		case "quit":
			cc = gomem.QUITQ
		case "flush_all":
			cc = gomem.FLUSHQ
		case "append":
			cc = gomem.APPENDQ
		case "prepend":
			cc = gomem.PREPENDQ
		}
	}
	return
}

func check_args(args []string, argv int) error {
	if len(args) != argv {
		return fmt.Errorf("CLIENT_ERROR wrong number of arguments (got %d, expected %d)\r\n", len(args), argv)
	}
	return nil
}

func strtm(in string) (uint32, error) {
	v, e := strconv.ParseUint(in, 10, 32)
	return uint32(v), e
}

func setargs(args []string, in *bufio.Reader) (flags uint32, exp uint32, value []byte, err error) {
	nbytes := args[len(args)-1]
	args = args[:len(args)-1]
	if len(args) >= 1 {
		var flags_ uint64
		flags_, err = strconv.ParseUint(args[0], 10, 16)
		if err != nil {
			return
		}
		flags = uint32(flags_)
	}

	if len(args) >= 2 {
		exp, err = strtm(args[1])
		if err != nil {
			return
		}
	}

	value, err = data(nbytes, in)
	return
}

func data(lenarg string, in *bufio.Reader) ([]byte, error) {
	ln, err := strconv.Atoi(lenarg)
	if err != nil {
		return nil, err
	}

	b := make([]byte, ln)
	_, err = io.ReadFull(in, b)
	if err == nil {
		// remove \r\n
		r, err := in.ReadByte()
		if err != nil {
			return nil, err
		}
		n, err := in.ReadByte()
		if err != nil {
			return nil, err
		}
		if r != '\r' || n != '\n' {
			return nil, errors.New("data not terminated by \\r\\n")
		}
	}
	return b, err
}

func ToMCRequest(cmd string, in *bufio.Reader) (req gomem.MCRequest, err error) {
	args := strings.Fields(strings.TrimSpace(cmd))
	cmd = args[0]
	args = args[1:]

	quiet := false
	isget := strings.HasPrefix(cmd, "get")
	if !isget && len(args) != 0 && args[len(args)-1] == "noreply" {
		quiet = true
		args = args[:len(args)-1]
	}

	req.Opcode = str2op(cmd, quiet)

	switch cmd {
	case "get", "gets":
		// MUST have key.
		req.Key = []byte(args[0])
		args = args[1:]
		// MUST NOT have extras.
		// MUST NOT have value.
	case "set", "add", "replace":
		// MUST have key.
		req.Key = []byte(args[0])
		args = args[1:]
		// MUST have extras.
		// - 4 byte flags
		// - 4 byte expiration time
		// MUST have value.

		nargs := 3 /* flags expiration bytes */
		if cmd == "cas" {
			nargs++ /* + cas id */
		}

		err = check_args(args, nargs)
		if err != nil {
			return
		}

		if cmd == "cas" {
			req.Cas, err = strconv.ParseUint(args[len(args)-1], 10, 64)
			if err != nil {
				return
			}
			args = args[:len(args)-1]
		}

		var flags uint32
		var exp uint32
		var val []byte
		flags, exp, val, err = setargs(args, in)
		if err != nil {
			return
		}

		req.Body = val
		req.Extras = make([]byte, 8)
		binary.BigEndian.PutUint32(req.Extras[0:4], flags)
		binary.BigEndian.PutUint32(req.Extras[4:8], exp)
	case "delete":
		// MUST have key.
		req.Key = []byte(args[0])
		args = args[1:]
		// MUST NOT have extras.
		// MUST NOT have value.
	case "incr", "decr":
		// MUST have key.
		req.Key = []byte(args[0])
		args = args[1:]
		// MUST have extras.
		// - 8 byte value to add / subtract
		// - 8 byte initial value (unsigned)
		// - 4 byte expiration time
		// MUST NOT have value.
		//
		// NOTE: binary protocol allows setting default and expiry for
		// incr/decr, but text protocol does not. We therefore set them
		// to 0 here to be correct.

		err = check_args(args, 1) /* amount */
		if err != nil {
			return
		}

		var by uint64
		by, err = strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return
		}

		req.Extras = make([]byte, 8+8+4)
		binary.BigEndian.PutUint64(req.Extras[0:8], by)

		binary.BigEndian.PutUint64(req.Extras[8:16], 0)
		binary.BigEndian.PutUint64(req.Extras[16:20], 0)
	case "quit":
		// MUST NOT have extras.
		// MUST NOT have key.
	case "flush_all":
		// MAY have extras.
		// - 4 byte expiration time
		// MUST NOT have key.
		// MUST NOT have value.
		//
		// TODO: handle optional "now" argument
	case "noop":
		// MUST NOT have extras.
		// MUST NOT have key.
		// MUST NOT have value.
	case "version":
		// MUST NOT have extras.
		// MUST NOT have key.
		// MUST NOT have value.
	case "append", "prepend":
		// MUST have key.
		req.Key = []byte(args[0])
		args = args[1:]
		// MUST NOT have extras.
		// MUST have value.

		err = check_args(args, 1)
		if err != nil {
			return
		}

		var val []byte
		_, _, val, err = setargs(args, in)
		if err != nil {
			return
		}

		req.Body = val
		// TODO: case "stat":
	}
	return
}
