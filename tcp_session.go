package tcpsession

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ZhangGuangxu/netbuffer"
)

const (
	readDuration  = 50 * time.Millisecond
	sleepDuration = 5 * time.Millisecond
	writeDuration = 50 * time.Millisecond
)

var (
	// 依据90%的消息的最大size决定
	tmpBufSize = 64
	// 依据最大request from client消息的size的2倍决定
	incomingHighWaterMark = 256
	// 依据最大response from server消息的size的100倍决定
	outgoingHighWaterMark = 100 * 1024
)

func SetTmpBufSize(s int) {
	tmpBufSize = s
}

func SetIncomingHighWaterMark(s int) {
	incomingHighWaterMark = s
}

func SetOutgoingHighWaterMark(s int) {
	outgoingHighWaterMark = s
}

// TCPSession is a wrapper of net.TCPConn and raw bytes buffers
type TCPSession struct {
	conn          *net.TCPConn
	quit          int64
	wg            *sync.WaitGroup
	readSuspended bool              // true-暂停read
	incoming      *netbuffer.Buffer // 接收网络数据的缓冲区
	outgoing      *netbuffer.Buffer // 将要发送的网络数据的缓冲区
}

// NewTCPSession
func NewTCPSession(conn *net.TCPConn) *TCPSession {
	return &TCPSession{
		conn:     conn,
		wg:       &sync.WaitGroup{},
		incoming: netbuffer.NewBuffer(),
		outgoing: netbuffer.NewBuffer(),
	}
}

// NewTCPSessionWithBuffSize
// @param inBuffSize is the size of incoming. 0 means to use default buff size.
// @param outBuffSize is the size of outgoing. 0 means to use default buff size.
func NewTCPSessionWithBuffSize(
	conn *net.TCPConn,
	inBuffSize, outBuffSize int) *TCPSession {
	return &TCPSession{
		conn:     conn,
		wg:       &sync.WaitGroup{},
		incoming: netbuffer.NewBufferWithSize(inBuffSize),
		outgoing: netbuffer.NewBufferWithSize(outBuffSize),
	}
}

func (t *TCPSession) Start() {
	t.wg.Add(2)
	go t.read()
	go t.write()
}

func (t *TCPSession) Wait() {
	t.wg.Wait()
	t.conn.Close()
	t.conn = nil
}

func (t *TCPSession) IsClosed() bool {
	return t.conn == nil
}

func (t *TCPSession) SetQuit() {
	atomic.StoreInt64(&t.quit, 1)
}

func (t *TCPSession) needQuit() bool {
	return atomic.LoadInt64(&t.quit) == 1
}

// read invokes TCPConn.Read
func (t *TCPSession) read() {
	defer t.wg.Done()
	defer t.SetQuit()
	tmpBuf := make([]byte, tmpBufSize)

	for {
		if t.needQuit() {
			break
		}
		if t.readSuspended && t.incoming.ReadableBytes() < incomingHighWaterMark {
			t.readSuspended = false
		}
		if t.readSuspended {
			time.Sleep(sleepDuration)
			continue
		}

		var buf []byte
		var useTmpBuf bool
		if t.incoming.WritableBytes() > 0 {
			buf = t.incoming.WritableByteSlice()
		} else {
			buf = tmpBuf
			useTmpBuf = true
		}
		if err := t.conn.SetReadDeadline(time.Now().Add(readDuration)); err != nil {
			// TODO: log err
			break
		}
		var eof bool
		n, err := t.conn.Read(buf)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				// nothing to do
			} else if err == io.EOF {
				// TODO: log debug
				eof = true
			} else {
				// TODO: log err
				break
			}
		}
		if n > 0 {
			if useTmpBuf {
				// ATTENTION: MUST specify the end index(here is n) of 'buf'!
				t.incoming.Append(buf[:n])
			}
		}
		if eof {
			break
		}
		if t.incoming.ReadableBytes() >= incomingHighWaterMark {
			t.readSuspended = true
		}
		if err := t.conn.SetReadDeadline(time.Time{}); err != nil {
			// TODO: log err
			break
		}
	}
}

// write invokes TCPConn.Write
func (t *TCPSession) write() {
	defer t.wg.Done()
	defer t.SetQuit()

	for {
		if t.needQuit() {
			break
		}
		if t.outgoing.ReadableBytes() <= 0 {
			time.Sleep(sleepDuration)
			continue
		}

		if err := t.conn.SetWriteDeadline(time.Now().Add(writeDuration)); err != nil {
			// TODO: log err
			break
		}
		n, err := t.conn.Write(t.outgoing.PeekAllAsByteSlice())
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				// nothing to do
			} else {
				// TODO: log err
				break
			}
		}
		if n > 0 {
			t.outgoing.Retrieve(n)
		}
		if err := t.conn.SetWriteDeadline(time.Time{}); err != nil {
			// TODO: log err
			break
		}

		if t.outgoing.ReadableBytes() >= outgoingHighWaterMark {
			// TODO: log warn
			break
		}
	}
}
