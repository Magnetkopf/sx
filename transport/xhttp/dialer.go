package xhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	sHTTP "github.com/sagernet/sing/protocol/http"
	"golang.org/x/net/http2"
)

func createHTTPClient(dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayXHTTPOptions, tlsConfig tls.Config) DialerClient {
	var transport http.RoundTripper
	httpVersion := "2"

	if tlsConfig == nil {
		httpVersion = "1.1"
		transport = &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, serverAddr)
			},
			// Keep-alive is required for upload reuse in packet-up mode
			DisableKeepAlives: false,
		}
	} else {
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
		}
		tlsDialer := tls.NewDialer(dialer, tlsConfig)

		negotiatedH1 := len(tlsConfig.NextProtos()) > 0 && tlsConfig.NextProtos()[0] == "http/1.1"
		if negotiatedH1 {
			httpVersion = "1.1"
			transport = &http.Transport{
				DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return tlsDialer.DialTLSContext(ctx, serverAddr)
				},
			}
		} else {
			transport = &http2.Transport{
				ReadIdleTimeout: time.Duration(options.IdleTimeout),
				PingTimeout:     time.Duration(options.PingTimeout),
				DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
					return tlsDialer.DialTLSContext(ctx, serverAddr)
				},
			}
		}
	}

	return &DefaultDialerClient{
		transportConfig: options,
		client: &http.Client{
			Transport: transport,
		},
		httpVersion: httpVersion,
	}
}

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func getNormalizedPath(path string) string {
	if path == "" || path[0] != '/' {
		path = "/" + path
	}
	if path[len(path)-1] != '/' {
		path = path + "/"
	}
	return path
}

func Dial(ctx context.Context, xmuxManager *XmuxManager, dialer N.Dialer, dest M.Socksaddr, options option.V2RayXHTTPOptions, tlsConfig tls.Config) (net.Conn, error) {
	var requestURL url.URL

	if tlsConfig != nil {
		requestURL.Scheme = "https"
	} else {
		requestURL.Scheme = "http"
	}

	requestURL.Host = options.Host
	if requestURL.Host == "" && tlsConfig != nil {
		requestURL.Host = tlsConfig.ServerName()
	}
	if requestURL.Host == "" {
		requestURL.Host = dest.String()
	}

	if err := sHTTP.URLSetPath(&requestURL, options.Path); err != nil {
		return nil, E.Cause(err, "parse path")
	}
	// Ensure path ends with / so session IDs get appended correctly.
	requestURL.Path = getNormalizedPath(requestURL.Path)

	mode := options.Mode
	if mode == "" || mode == "auto" {
		// REALITY TLS prefers stream-one (same as Xray-core's auto logic).
		// We detect Reality by checking if it is a Reality TLS implementation.
		if isRealityTLS(tlsConfig) {
			mode = "stream-one"
		} else {
			mode = "packet-up"
		}
	}

	sessionId := ""
	if mode != "stream-one" {
		sessionId = newUUID()
	}

	xmuxClient := xmuxManager.GetXmuxClient(ctx)
	xmuxClient.OpenUsage.Add(1)

	httpClient := xmuxClient.XmuxConn.(DialerClient)

	conn := &splitConn{
		onClose: func() {
			xmuxClient.OpenUsage.Add(-1)
		},
	}

	var err error

	switch mode {
	case "stream-one":
		// Single bidirectional stream: upload as POST body, download as response body.
		uploadPipeReader, uploadPipeWriter := io.Pipe()
		conn.writer = uploadPipeWriter
		xmuxClient.LeftRequests.Add(-1)
		conn.reader, conn.remoteAddr, conn.localAddr, err = httpClient.OpenStream(ctx, requestURL.String(), sessionId, uploadPipeReader, false)
		if err != nil {
			uploadPipeWriter.Close()
			return nil, err
		}

	case "stream-up":
		// Upload is a long-lived POST; download is a GET response.
		xmuxClient.LeftRequests.Add(-1)
		conn.reader, conn.remoteAddr, conn.localAddr, err = httpClient.OpenStream(ctx, requestURL.String(), sessionId, nil, false)
		if err != nil {
			return nil, err
		}
		uploadPipeReader, uploadPipeWriter := io.Pipe()
		conn.writer = uploadPipeWriter
		xmuxClient.LeftRequests.Add(-1)
		go func() {
			_, _, _, err := httpClient.OpenStream(ctx, requestURL.String(), sessionId, uploadPipeReader, true)
			if err != nil {
				uploadPipeWriter.CloseWithError(err)
			}
		}()

	default: // packet-up (default)
		// Download: single long-lived GET.
		xmuxClient.LeftRequests.Add(-1)
		conn.reader, conn.remoteAddr, conn.localAddr, err = httpClient.OpenStream(ctx, requestURL.String(), sessionId, nil, false)
		if err != nil {
			return nil, err
		}

		maxUploadSize := int32(1000000)
		if options.Extra != nil && options.Extra.ScMaxEachPostBytes != "" {
			maxUploadSize = parseRangeConfig(options.Extra.ScMaxEachPostBytes).rand()
		}

		intervalMs := int32(30)
		if options.Extra != nil && options.Extra.ScMinPostsIntervalMs != "" {
			intervalMs = parseRangeConfig(options.Extra.ScMinPostsIntervalMs).rand()
		}

		// uploadPipeWriter is what the proxy layer writes into.
		// A goroutine reads out chunks and POSTs them.
		uploadPipeReader, uploadPipeWriter := io.Pipe()
		conn.writer = uploadPipeWriter

		go func() {
			var seq int64
			var lastWrite time.Time
			buf := make([]byte, maxUploadSize)

			for {
				urlCopy := requestURL
				seqStr := strconv.FormatInt(seq, 10)
				seq++

				if intervalMs > 0 {
					sleepDuration := time.Duration(intervalMs)*time.Millisecond - time.Since(lastWrite)
					if sleepDuration > 0 {
						time.Sleep(sleepDuration)
					}
				}

				n, err := uploadPipeReader.Read(buf)
				if err != nil {
					break
				}

				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				lastWrite = time.Now()

				// Refresh xmux client if exhausted
				if xmuxClient.LeftRequests.Add(-1) <= 0 ||
					(!xmuxClient.UnreusableAt.IsZero() && lastWrite.After(xmuxClient.UnreusableAt)) {
					xmuxClient = xmuxManager.GetXmuxClient(ctx)
					httpClient = xmuxClient.XmuxConn.(DialerClient)
				}

				go func(seqStr, urlStr string, data []byte) {
					err := httpClient.PostPacket(
						ctx,
						urlStr,
						sessionId,
						seqStr,
						bytes.NewReader(data),
						int64(len(data)),
					)
					if err != nil {
						uploadPipeReader.CloseWithError(err)
					}
				}(seqStr, urlCopy.String(), chunk)
			}
		}()
	}

	return conn, nil
}

// isRealityTLS returns true when tlsConfig is a REALITY client config.
// Detection: RealityClientConfig.STDConfig() always returns error;
// regular TLS/uTLS configs return a valid *tls.Config.
func isRealityTLS(tlsConfig tls.Config) bool {
	if tlsConfig == nil {
		return false
	}
	_, err := tlsConfig.STDConfig()
	return err != nil
}
