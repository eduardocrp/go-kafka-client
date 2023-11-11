//go:build consumer

package main

import (
	"fmt"
	"go-kafka-client/environment"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
)

type Sample struct {
	Campo1 int
	Campo2 float64
	Campo3 string
	Campo4 float64
}

func main() {
	conf := environment.ConfigKakfa()

	conf["group.id"] = "test-group-id"
	conf["client.id"] = "test-group-id_consumer"
	conf["auto.offset.reset"] = "latest"

	topic := "test_idempotence_key"
	c, err := kafka.NewConsumer(&conf)
	if err != nil {
		log.Panic(err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	avroSchemaFile, err := os.ReadFile("avro/sample.avsc")
	if err != nil {
		panic(err)
	}

	schema, err := goavro.NewCodec(string(avroSchemaFile))
	if err != nil {
		panic(err)
	}

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(1 * time.Second)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("%v - Consumed event from topic %s: partição=%d offset=%d key = %s",
				time.Now(), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, string(ev.Key))

			value, _, err := schema.NativeFromBinary(ev.Value)
			if err != nil {
				panic(err.Error())
			}
			valueResult := value.(map[string]interface{})

			campo1, _ := valueResult["my_field1"].(int32)
			campo2, _ := valueResult["my_field2"].(float64)
			campo3, _ := valueResult["my_field3"].(string)
			campo4S, _ := valueResult["my_field4"].(string)
			campo4, _ := strconv.ParseFloat(campo4S, 64)

			result := Sample{
				Campo1: int(campo1),
				Campo2: campo2,
				Campo3: campo3,
				Campo4: campo4,
			}

			fmt.Printf(" value=%v\n", result)
		}

		//time.Sleep(time.Second * 15)
	}

	c.Close()
}
