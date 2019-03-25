package main

import (
	"log"

	"github.com/dwin/nsq-example/base"
	"github.com/icrowley/fake"

	jsoniter "github.com/json-iterator/go"

	"github.com/nsqio/go-nsq"
)

const topic = "testing"

func main() {

	cfg := nsq.NewConfig()
	cfg.Snappy = true

	nsqAddr := "localhost:4150"

	prod, err := nsq.NewProducer(nsqAddr, cfg)
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < 10000; i++ {
		payload := base.Payload{
			Message: fake.Sentences(),
		}
		body, err := jsoniter.Marshal(&payload)
		if err != nil {
			log.Println(err)
		}

		if err := prod.Publish(topic, body); err != nil {
			log.Printf("Publish error: %s\n", err)
		}
	}

}
