package kafka

import (
	"context"
	"fmt"

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
		fetches := c.client.PollRecords(ctx, 5)
		if errs := fetches.Errors(); len(errs) > 0 {
			zapctx.Error(ctx, "error reading message, trying again")

			continue
		}

		fmt.Println("Pool size: ", len(fetches.Records()))

		fetches.EachRecord(func(r *kgo.Record) {
			err := processor(ctx, r)
			if err != nil {
				zapctx.Error(ctx, "error processing message", zap.Error(err))

				return
			}

			zapctx.Info(ctx, "message processed")
		})

		if err := c.client.CommitUncommittedOffsets(context.Background()); err != nil {
			zapctx.Error(ctx, "commit records failed", zap.Error(err))
			continue
		}
	}
}

func (c *Client) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}
