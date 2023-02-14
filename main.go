package main

import (
	"confluent-kafka/util"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Confluent Kafka")
	//Read file from Environment variable
	kafkaConfigFileLocation := util.GetEnv("BKAAS_KAFKA_CONFIG", "/tmp/kafka.config")
	fmt.Println(fmt.Sprintf("Kafka  config file path : %s", kafkaConfigFileLocation))
	// take topic from command line arguments
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topic-name>\n",
			os.Args[0])
		os.Exit(1)
	}
	topicName := os.Args[1]
	//topicName := "bkaas_bo_corpaction"
	kafkaConsumer(kafkaConfigFileLocation, topicName)

}

func kafkaConsumer(kafkaConfigFileLocation string, topicName string) {
	conf := util.ReadConfig(kafkaConfigFileLocation)
	conf["group.id"] = "kafka-go-getting-started"
	conf["auto.offset.reset"] = "earliest"

	c, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics([]string{topicName}, nil)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {

				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}

	c.Close()
}
