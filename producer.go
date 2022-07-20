package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (c *Client) WriteMessage(ctx context.Context, rec *kgo.Record) error {
	return c.client.ProduceSync(ctx, rec).FirstErr()
}
