//go:build producer

package main

import (
	"fmt"
	"go-kafka-client/environment"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
)

func main() {
	conf := environment.ConfigKakfa()

	conf["delivery.timeout.ms"] = 45000
	conf["enable.idempotence"] = true
	conf["acks"] = "all"

	topic := "test_idempotence_key"
	p, err := kafka.NewProducer(&conf)
	if err != nil {
		log.Panic(err)
	}
	defer p.Close()

	avroMessage := map[string]interface{}{
		"my_field4": "123.456",
		"my_field3": "test",
		"my_field2": 20000000000,
		"my_field1": 1,
	}

	avroSchemaFile, err := os.ReadFile("avro/sample.avsc")
	if err != nil {
		panic(err)
	}

	schema, err := goavro.NewCodec(string(avroSchemaFile))
	if err != nil {
		panic(err)
	}

	binaryData, err := schema.BinaryFromNative(nil, avroMessage)
	if err != nil {
		panic(err)
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("6"),
		Value: binaryData,
	}

	err = p.Produce(msg, nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Produzido em %s com sucesso para key %s\n", string(*msg.TopicPartition.Topic), string(msg.Key))

	p.Flush(5000)
}
