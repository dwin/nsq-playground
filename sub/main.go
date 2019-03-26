package main

import (
	"fmt"
	"time"

	"github.com/dwin/nsq-playground/base"

	"log"

	jsoniter "github.com/json-iterator/go"

	"github.com/nsqio/go-nsq"
)

const topic = "testing"
const channel = "test"

func main() {
	cfg := nsq.NewConfig()
	cfg.Snappy = true

	nsqLookupAddr := "localhost:4150"

	cons, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		log.Fatalln(err)
	}

	cons.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var payload base.Payload
		if err := jsoniter.Unmarshal(message.Body, &payload); err != nil {
			log.Println(err)
		}

		fmt.Printf("ID %s : %s\n", message.ID, payload.Message)

		return nil
	}))

	if err := cons.ConnectToNSQD(nsqLookupAddr); err != nil {
		log.Fatalln(err)
	}

	time.Sleep(time.Second * 60)

	cons.Stop()

}
