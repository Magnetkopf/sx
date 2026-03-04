package xhttp

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/option"
)

type RangeConfig struct {
	From int64
	To   int64
}

func parseRangeConfig(s string) RangeConfig {
	if s == "" {
		return RangeConfig{0, 0}
	}
	parts := strings.SplitN(s, "-", 2)
	from, _ := strconv.ParseInt(parts[0], 10, 64)
	if len(parts) == 1 {
		return RangeConfig{From: from, To: from}
	}
	to, _ := strconv.ParseInt(parts[1], 10, 64)
	return RangeConfig{From: from, To: to}
}

func (c RangeConfig) rand() int32 {
	if c.From == c.To {
		return int32(c.From)
	}
	max := big.NewInt(c.To - c.From + 1)
	r, _ := rand.Int(rand.Reader, max)
	return int32(c.From + r.Int64())
}

func getNormalizedMaxConcurrency(opt option.V2RayXHTTPXmuxOptions) RangeConfig {
	return parseRangeConfig(opt.MaxConcurrency)
}
func getNormalizedMaxConnections(opt option.V2RayXHTTPXmuxOptions) RangeConfig {
	return parseRangeConfig(opt.MaxConnections)
}
func getNormalizedCMaxReuseTimes(opt option.V2RayXHTTPXmuxOptions) RangeConfig {
	return parseRangeConfig(opt.CMaxReuseTimes)
}
func getNormalizedHMaxRequestTimes(opt option.V2RayXHTTPXmuxOptions) RangeConfig {
	return parseRangeConfig(opt.HMaxRequestTimes)
}
func getNormalizedHMaxReusableSecs(opt option.V2RayXHTTPXmuxOptions) RangeConfig {
	return parseRangeConfig(opt.HMaxReusableSecs)
}

type XmuxConn interface {
	IsClosed() bool
}

type XmuxClient struct {
	XmuxConn     XmuxConn
	OpenUsage    atomic.Int32
	leftUsage    int32
	LeftRequests atomic.Int32
	UnreusableAt time.Time
}

type XmuxManager struct {
	xmuxConfig  option.V2RayXHTTPXmuxOptions
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
}

func NewXmuxManager(xmuxConfig option.V2RayXHTTPXmuxOptions, newConnFunc func() XmuxConn) *XmuxManager {
	return &XmuxManager{
		xmuxConfig:  xmuxConfig,
		concurrency: getNormalizedMaxConcurrency(xmuxConfig).rand(),
		connections: getNormalizedMaxConnections(xmuxConfig).rand(),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
	}
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
	}
	if x := getNormalizedCMaxReuseTimes(m.xmuxConfig).rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := getNormalizedHMaxRequestTimes(m.xmuxConfig).rand(); x > 0 {
		xmuxClient.LeftRequests.Store(x)
	}
	if x := getNormalizedHMaxReusableSecs(m.xmuxConfig).rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	return xmuxClient
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient {
	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		if xmuxClient.XmuxConn.IsClosed() ||
			xmuxClient.leftUsage == 0 ||
			xmuxClient.LeftRequests.Load() <= 0 ||
			(!xmuxClient.UnreusableAt.IsZero() && time.Now().After(xmuxClient.UnreusableAt)) {
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
		} else {
			i++
		}
	}

	if len(m.xmuxClients) == 0 {
		return m.newXmuxClient()
	}

	if m.connections > 0 && len(m.xmuxClients) < int(m.connections) {
		return m.newXmuxClient()
	}

	xmuxClients := make([]*XmuxClient, 0)
	if m.concurrency > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.OpenUsage.Load() < m.concurrency {
				xmuxClients = append(xmuxClients, xmuxClient)
			}
		}
	} else {
		xmuxClients = m.xmuxClients
	}

	if len(xmuxClients) == 0 {
		return m.newXmuxClient()
	}

	i, _ := rand.Int(rand.Reader, big.NewInt(int64(len(xmuxClients))))
	xmuxClient := xmuxClients[i.Int64()]
	if xmuxClient.leftUsage > 0 {
		xmuxClient.leftUsage -= 1
	}
	return xmuxClient
}
