package kafka

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type mod string

const (
	MOD_CONSUMER mod = "consumer"
	MOD_PRODUCER mod = "producer"
	MOD_ULTRA    mod = "ultra"
)

type ClientConfig struct {
	Brokers        []string
	Group          string
	Topic          string
	Username       string
	Password       string
	TickerDuration time.Duration
	startOffset    int64
}

type Client struct {
	client *kgo.Client

	config ClientConfig
	mod    mod
}

func NewClient(config ClientConfig, client_mod mod) (*Client, error) {
	if !(client_mod == MOD_CONSUMER || client_mod == MOD_PRODUCER || client_mod == MOD_ULTRA) {
		return nil, fmt.Errorf("unknown client mode")
	}

	switch {
	case config.Topic == "":
		return nil, fmt.Errorf("empty topic")
	case config.Group == "":
		return nil, fmt.Errorf("empty group ID")
	case len(config.Brokers) == 0:
		return nil, fmt.Errorf("empty brokers")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.Group),
		kgo.ConsumeTopics(config.Topic),
		kgo.DefaultProduceTopic(config.Topic),
		kgo.AllowAutoTopicCreation(),
	}

	if config.Username != "" && config.Password != "" {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.SASL(plain.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsMechanism()),
			kgo.Dialer(tlsDialer.DialContext),
		)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	return &Client{client: client, config: config, mod: client_mod}, nil
}

func (c *Client) Close() {
	c.client.Close()
}
