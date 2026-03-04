package xhttp

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

type DialerClient interface {
	IsClosed() bool
	// ctx, url, sessionId, body (nil = GET / download only), uploadOnly
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)
	// ctx, url, sessionId, seqStr, body, contentLength
	PostPacket(context.Context, string, string, string, io.Reader, int64) error
}

type DefaultDialerClient struct {
	transportConfig option.V2RayXHTTPOptions
	client          *http.Client
	closed          atomic.Bool
	httpVersion     string
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, urlStr string, sessionId string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	// gotConn is used to unblock Dial once the TCP handshake is done so we
	// can return correct addresses. For uploadOnly streams we don't need to
	// wait for GotConn.
	gotConn := make(chan struct{})
	var gotConnOnce atomic.Bool

	closeGotConn := func() {
		if gotConnOnce.CompareAndSwap(false, true) {
			close(gotConn)
		}
	}

	innerCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			remoteAddr = info.Conn.RemoteAddr()
			localAddr = info.Conn.LocalAddr()
			closeGotConn()
		},
	})

	method := "GET"
	if body != nil {
		method = "POST"
	}

	req, err := http.NewRequestWithContext(context.WithoutCancel(innerCtx), method, urlStr, body)
	if err != nil {
		return nil, nil, nil, E.Cause(err, "build request")
	}

	for k, v := range c.transportConfig.Headers.Build() {
		for _, v2 := range v {
			req.Header.Add(k, v2)
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}

	applyMetaToRequest(&c.transportConfig, req, sessionId, "")

	if method == "POST" {
		req.Header.Set("Content-Type", "application/grpc")
	}

	waitRC := &WaitReadCloser{Wait: make(chan struct{})}
	wrc = waitRC

	go func() {
		fmt.Println("[DEBUG] xhttp OpenStream: starting Do() for", urlStr, "uploadOnly=", uploadOnly)
		resp, err := c.client.Do(req)

		closeGotConn()
		if err != nil {
			fmt.Println("[ERROR] xhttp OpenStream: Do() error:", err)
			if !uploadOnly {
				c.closed.Store(true)
			}
			waitRC.Close()
			return
		}

		fmt.Println("[DEBUG] xhttp OpenStream: response status", resp.StatusCode)
		if resp.StatusCode != 200 || uploadOnly {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			waitRC.Close()
			return
		}

		fmt.Println("[DEBUG] xhttp OpenStream: Set response body")
		waitRC.Set(resp.Body)
	}()

	go func() {
		// fallback to unblock any future Wait if GotConn never fires
		<-waitRC.Wait
		closeGotConn()
	}()

	return wrc, nil, nil, nil
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, urlStr string, sessionId string, seqStr string, body io.Reader, contentLength int64) error {
	method := "POST"
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, urlStr, body)
	if err != nil {
		return err
	}
	req.ContentLength = contentLength

	for k, v := range c.transportConfig.Headers.Build() {
		for _, v2 := range v {
			req.Header.Add(k, v2)
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0")
	}

	applyMetaToRequest(&c.transportConfig, req, sessionId, seqStr)

	resp, err := c.client.Do(req)
	if err != nil {
		c.closed.Store(true)
		return err
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad status code: %s", resp.Status)
	}

	return nil
}

func applyMetaToRequest(config *option.V2RayXHTTPOptions, req *http.Request, sessionId string, seqStr string) {
	if sessionId != "" {
		req.URL.Path = appendToPath(req.URL.Path, sessionId)
	}
	if seqStr != "" {
		req.URL.Path = appendToPath(req.URL.Path, seqStr)
	}

	b := make([]byte, 2)
	rand.Read(b)
	paddingLen := 100 + int(b[0]) + int(b[1]) // ~100 to 610 bytes padding
	paddingStr := strings.Repeat("X", paddingLen)

	req.Header.Set("Referer", req.URL.String()+"?x_padding="+paddingStr)
}

func appendToPath(path, value string) string {
	if len(path) > 0 && path[len(path)-1] == '/' {
		return path + value
	}
	return path + "/" + value
}
