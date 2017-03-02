package main

import (
	"net/http"
	"log"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"time"
	"strconv"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"flag"
)

type KafkaClient struct {
	broker   string
	topic    string
	producer *kafka.Producer
}

func (kafkaClient *KafkaClient)open() {
	var err error
	kafkaClient.producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaClient.broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", kafkaClient.producer)
	go func() {
		for e := range kafkaClient.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

}

func (kafkaClient *KafkaClient)close() {
	kafkaClient.producer.Close()
}

func (kafkaClient *KafkaClient) sendToKafka(key, message []byte) {
	print("pushing event")
	kafkaClient.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaClient.topic, Partition: kafka.PartitionAny},
		Value: message}

	print("published")

}

func main() {
	topic := flag.String("topic", "github", "Kafka topic to use")
	broker := flag.String("broker", "localhost", "Address of hte kafka broker")
	username := flag.String("username", "elek", "Github username")
	password := flag.String("password", "", "Github password")
	url := flag.String("url", "https://api.github.com/users/me/events/orgs/apache", "github event url to parse")
	flag.Parse()

	client := &http.Client{}
	producer := KafkaClient{broker:*broker, topic:*topic}
	producer.open()
	req, err := http.NewRequest("GET", *url + "?per_page=100", nil)
	if err != nil {
		log.Fatal("Can't create request", err)
	}

	req.SetBasicAuth(*username, *password)
	lastid := int64(0)
	for {
		println("Checking github")
		resp, err := client.Do(req)

		if err != nil {
			log.Fatal("Can't get the github feed", err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			log.Fatal("Body is not readable", err)
		}

		var result []map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			log.Fatal("Can't parse json", err)
		}
		maxid := int64(0)
		for _, c := range result {
			id, _ := strconv.ParseInt(c["id"].(string), 10, 64)
			if (id > lastid) {
				eventType := string(c["type"].(string))
				jsonsection, _ := json.Marshal(c)
				print(string(jsonsection))
				println(eventType)
				producer.sendToKafka(jsonsection)

			}
			if (id > maxid ) {
				maxid = id
			}

		}
		lastid = maxid

		fmt.Sprintf("%T", result)
		time.Sleep(60 * time.Second)
	}
	producer.close()

}
