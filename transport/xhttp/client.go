package xhttp

import (
	"context"
	"net"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

type Client struct {
	ctx         context.Context
	dialer      N.Dialer
	serverAddr  M.Socksaddr
	options     option.V2RayXHTTPOptions
	tlsConfig   tls.Config
	xmuxManager *XmuxManager
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayXHTTPOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	c := &Client{
		ctx:        ctx,
		dialer:     dialer,
		serverAddr: serverAddr,
		options:    options,
		tlsConfig:  tlsConfig,
	}

	xmuxConfig := option.V2RayXHTTPXmuxOptions{}
	if options.Extra != nil && options.Extra.Xmux != nil {
		xmuxConfig = *options.Extra.Xmux
	}

	c.xmuxManager = NewXmuxManager(xmuxConfig, func() XmuxConn {
		return createHTTPClient(c.dialer, c.serverAddr, options, tlsConfig)
	})

	return c, nil
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	return c.DialXHTTP(ctx, c.serverAddr, c.options)
}

func (c *Client) DialXHTTP(ctx context.Context, dest M.Socksaddr, options option.V2RayXHTTPOptions) (net.Conn, error) {
	return Dial(ctx, c.xmuxManager, c.dialer, dest, options, c.tlsConfig)
}

func (c *Client) Close() error {
	// Let the garbage collector sweep the connections or implement explicit close if needed
	return nil
}
