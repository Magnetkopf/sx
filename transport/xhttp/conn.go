package xhttp

import (
	"io"
	"net"
	"time"
)

// splitConn is a net.Conn that wraps separate read and write channels.
// It is used by all XHTTP modes: the reader comes from the HTTP response
// body, while the writer feeds into whatever upload mechanism is selected.
type splitConn struct {
	reader     io.ReadCloser
	writer     io.WriteCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()
}

func (c *splitConn) Read(b []byte) (int, error) {
	if c.reader == nil {
		return 0, io.EOF
	}
	return c.reader.Read(b)
}

func (c *splitConn) Write(b []byte) (int, error) {
	if c.writer == nil {
		return 0, io.ErrClosedPipe
	}
	return c.writer.Write(b)
}

func (c *splitConn) Close() error {
	var firstErr error
	if c.reader != nil {
		firstErr = c.reader.Close()
	}
	if c.writer != nil {
		c.writer.Close()
	}
	if c.onClose != nil {
		c.onClose()
	}
	return firstErr
}

func (c *splitConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *splitConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *splitConn) SetDeadline(t time.Time) error      { return nil }
func (c *splitConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *splitConn) SetWriteDeadline(t time.Time) error { return nil }

// WaitReadCloser blocks Read until Set() is called with the real ReadCloser.
type WaitReadCloser struct {
	Wait chan struct{}
	io.ReadCloser
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.ReadCloser = rc
	defer func() {
		// Recover if the channel was already closed (by a concurrent Close).
		if recover() != nil {
			rc.Close()
		}
	}()
	close(w.Wait)
}

func (w *WaitReadCloser) Read(b []byte) (int, error) {
	if w.ReadCloser == nil {
		if <-w.Wait; w.ReadCloser == nil {
			return 0, io.ErrClosedPipe
		}
	}
	return w.ReadCloser.Read(b)
}

func (w *WaitReadCloser) Close() error {
	if w.ReadCloser != nil {
		return w.ReadCloser.Close()
	}
	defer func() {
		if recover() != nil && w.ReadCloser != nil {
			w.ReadCloser.Close()
		}
	}()
	close(w.Wait)
	return nil
}
