package kafka

import (
	"context"

	zapctx "github.com/saltpay/go-zap-ctx"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type Processor func(ctx context.Context, message *kgo.Record) error

func (c *Client) Listen(ctx context.Context, processor Processor) {
	if !(c.mod == MOD_CONSUMER || c.mod == MOD_ULTRA) {
		zapctx.Error(ctx, "can't open Listener in this mode", zap.String("mod", string(c.mod)))
		return
	}

	for {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			zapctx.Error(ctx, "error reading message, trying again")

			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			err := processor(ctx, record)
			if err != nil {
				zapctx.Error(ctx, "error processing message, message not committed", zap.Error(err))

				continue
			}

			zapctx.Info(ctx, "message processed")
		}
	}
}

func (c *Client) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}
