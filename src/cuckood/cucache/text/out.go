package text

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	gomem "github.com/dustin/gomemcached"
)

func WriteMCResponse(res *gomem.MCResponse, out io.Writer) (err error) {
	if res.Opcode.IsQuiet() && res.Opcode != gomem.GETKQ && res.Status == gomem.SUCCESS {
		// there is absolutely no reason to reply here
		return nil
	}

	switch res.Status {
	case gomem.SUCCESS:
		switch res.Opcode {
		case gomem.GETK, gomem.GETKQ:
			flags := binary.BigEndian.Uint32(res.Extras[0:4])
			_, err = out.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", res.Key, flags, len(res.Body), res.Cas)))
			if err != nil {
				return
			}
			_, err = out.Write(res.Body)
			if err != nil {
				return
			}
			_, err = out.Write([]byte{'\r', '\n'})
			if err != nil {
				return
			}
			if res.Opcode == gomem.GETK {
				_, err = out.Write([]byte("END\r\n"))
			}
		case gomem.SET, gomem.ADD, gomem.REPLACE:
			_, err = out.Write([]byte("STORED\r\n"))
		case gomem.DELETE:
			_, err = out.Write([]byte("DELETED\r\n"))
		case gomem.INCREMENT, gomem.DECREMENT:
			v := binary.BigEndian.Uint64(res.Body)
			_, err = out.Write([]byte(strconv.FormatUint(v, 10) + "\r\n"))
		}
	case gomem.KEY_ENOENT:
		if res.Opcode == gomem.GETK {
			_, err = out.Write([]byte("END\r\n"))
		} else if res.Opcode == gomem.GETKQ {
		} else {
			_, err = out.Write([]byte("NOT_FOUND\r\n"))
		}
	case gomem.KEY_EEXISTS:
		_, err = out.Write([]byte("EXISTS\r\n"))
	case gomem.NOT_STORED:
		_, err = out.Write([]byte("NOT_STORED\r\n"))
	case gomem.ENOMEM:
		_, err = out.Write([]byte("SERVER_ERROR no space for new entry\r\n"))
	case gomem.DELTA_BADVAL:
		_, err = out.Write([]byte("CLIENT_ERROR incr/decr on non-numeric field\r\n"))
	case gomem.UNKNOWN_COMMAND:
		_, err = out.Write([]byte("ERROR\r\n"))
	}
	return
}
