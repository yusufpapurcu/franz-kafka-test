package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	kafka "github.com/yusufpapurcu/franz-kafka-test"
)

func main() {
	client, err := kafka.NewClient(kafka.ClientConfig{Topic: "test-go-brr", Brokers: []string{"kafka:9093"}, Group: "test123"}, kafka.MOD_CONSUMER)
	if err != nil {
		panic(err)
	}

	client.Listen(context.TODO(), func(_ context.Context, message *kgo.Record) error {
		fmt.Println(string(message.Value))
		return nil
	})
}
