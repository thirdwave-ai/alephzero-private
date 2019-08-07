package alephzero

// #cgo CFLAGS: -Iinclude
// #include "go/alephzero_go.h"
import "C"

import (
	"log"
)

var (
	// TODO: make thread safe.
	allocRegistry map[int]func(C.size_t, *C.a0_buf_t)
	nextAllocId   int
)

func errorFrom(err C.errno_t) error {
	if err == 0 {
		return nil
	}
	return syscall.Errno(err).Error()
}

//export a0go_alloc
func a0go_alloc(idPtr unsafe.Pointer, size C.size_t, out *C.a0_buf_t) {
	allocRegistry[*((*int)idPtr)](size, out)
}

func registerAlloc(fn func(C.size_t, *C.a0_buf_t)) int {
	id := nextAllocId++
	allocRegistry[id] = fn
}

func unregisterAlloc(id int) {
	delete(allocRegistry, id)
}

type Packet struct {
	cPkt  C.a0_packet_t
	goMem []byte
}

type Packet = C.a0_packet_t

func NewPacket(headers map[string]string, payload []byte) (pkt Packet, err error) {
	hdrs := make([]C.a0_packet_header_t, len(headers))

	// TODO: What if payload is 0?
	var c_payload C.a0_buf_t
	c_payload.size = C.size_t(len(payload))
	c_payload.ptr = unsafe.Pointer(&payload[0])

	i := 0
	for k, v := range headers {
		hdrs[i].key.size = C.size_t(len(k))
		hdrs[i].key.ptr = unsafe.Pointer(&k[0])
		hdrs[i].val.size = C.size_t(len(v))
		hdrs[i].val.ptr = unsafe.Pointer(&v[0])
		i++
	}

	allocId := registerAlloc(func(size C.size_t, out *C.a0_buf_t) {
		pkt.goMem = make([]byte, int(size))
		out = &data.goMem[0]
	})
	defer unregisterAlloc(allocId)

	err = errorFrom(C.a0go_packet_build(
		C.size_t(len(headers)),
		unsafe.Pointer(&hdrs[0]),
		c_payload,
		allocId,
		&pkt.cPkt))
}

func (p *Packet) Bytes() ([]byte, error) {
	return p.goMem, nil
}

func (p *Packet) NumHeader() (cnt int, err error) {
	err = errorFrom(C.a0_packet_num_headers(p.cPkt, (*C.size_t)&cnt))
}

func (p *Packet) Header(idx int) (key, val string, err error) {
	var hdr C.a0_packet_header_t

	if err = errorFrom(a0_packet_header(p.cPkt, C.size_t(idx), &hdr)); err != nil {
		return
	}

	key = string((*[1<<30]byte)(unsafe.Pointer(hdr.key.ptr))[:int(hdr.key.size):int(hdr.key.size)])
	val = string((*[1<<30]byte)(unsafe.Pointer(hdr.val.ptr))[:int(hdr.val.size):int(hdr.val.size)])
}

func (p *Packet) Payload() (payload []byte, err error) {
	var out C.a0_buf_t

	if err = errorFrom(a0_packet_header(p.cPkt, C.size_t(idx), &hdr)); err != nil {
		return
	}

	payload = (*[1<<30]byte)(unsafe.Pointer(out.ptr))[:int(out.size):int(out.size)]
}

func (p *Packet) Headers() (map[string]string, error) {
	numHdrs, err := p.NumHeader()
	if err != nil {
		return nil, err
	}

	hdrs := make(map[string]string)
	for i := 0; i < numHdrs; i++ {
		k, v, err := p.Header(i)
		if err != nil {
			return nil, err
		}
		hdrs[k] = v
	}
	return hdrs, nil
}
