package text

import (
	"encoding/binary"
	"io"
	"strconv"
	"strings"

	gomem "github.com/dustin/gomemcached"
)

func ToMCRequest(cmd string, in io.Reader) (reqs []gomem.MCRequest, err error) {
	args := strings.Fields(strings.TrimSpace(cmd))
	cmd = args[0]
	args = args[1:]

	quiet := false
	isget := strings.HasPrefix(cmd, "get")
	if !isget && len(args) != 0 && args[len(args)-1] == "noreply" {
		quiet = true
		args = args[:len(args)-1]
	}

	if cmd == "get" || cmd == "gets" {
		reqs = make([]gomem.MCRequest, 0, len(args))
		for _, k := range args[:len(args)-1] {
			// MUST have key.
			// MUST NOT have extras.
			// MUST NOT have value.
			reqs = append(reqs, gomem.MCRequest{
				Opcode: gomem.GETKQ,
				Key:    []byte(k),
			})
		}
		reqs = append(reqs, gomem.MCRequest{
			Opcode: gomem.GETK,
			Key:    []byte(args[len(args)-1]),
		})
		return
	}

	reqs = make([]gomem.MCRequest, 1)
	req := &reqs[0]
	req.Opcode = str2op(cmd, quiet)

	switch cmd {
	case "set", "cas", "add", "replace":
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
		by, err = strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return
		}

		req.Extras = make([]byte, 8+8+4)
		binary.BigEndian.PutUint64(req.Extras[0:8], by)
		binary.BigEndian.PutUint64(req.Extras[8:16], 0)

		/*
		 * the item must already exist for incr/decr to work; these
		 * commands won't pretend that a non-existent key exists with
		 * value 0; instead, they will fail.
		 */
		binary.BigEndian.PutUint32(req.Extras[16:20], 0xffffffff)
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
