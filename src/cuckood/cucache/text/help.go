package text

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	gomem "github.com/dustin/gomemcached"
)

func str2op(cmd string, quiet bool) (cc gomem.CommandCode) {
	cc = 0xff
	if !quiet {
		switch cmd {
		case "set", "cas":
			cc = gomem.SET
		case "get", "gets":
			cc = gomem.GETK
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
		case "set", "cas":
			cc = gomem.SETQ
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

func setargs(args []string, in io.Reader) (flags uint32, exp uint32, value []byte, err error) {
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

func data(lenarg string, in io.Reader) ([]byte, error) {
	ln, err := strconv.Atoi(lenarg)
	if err != nil {
		return nil, err
	}

	b := make([]byte, ln)
	_, err = io.ReadFull(in, b)
	if err == nil {
		// remove \r\n
		rn := make([]byte, 2)
		_, err = io.ReadFull(in, rn)
		if err != nil {
			return nil, err
		}
		if rn[0] != '\r' || rn[1] != '\n' {
			return nil, errors.New("data not terminated by \\r\\n")
		}
	}
	return b, err
}
