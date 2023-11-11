//go:build dev

package environment

import "github.com/confluentinc/confluent-kafka-go/kafka"

func ConfigKakfa() kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": "HOST",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     "USERNAME",
		"sasl.password":     "PASSWORD",
	}
}
