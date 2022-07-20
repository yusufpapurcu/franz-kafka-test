package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	kafka "github.com/yusufpapurcu/franz-kafka-test"
)

func main() {
	client, err := kafka.NewClient(kafka.ClientConfig{Topic: "test-go-brr", Brokers: []string{"kafka:9093"}, Group: "test123"}, kafka.MOD_PRODUCER)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err := client.WriteMessage(context.TODO(), kgo.StringRecord(fmt.Sprintf("message %d", i)))
		if err != nil {
			panic(err)
		}
	}
}
