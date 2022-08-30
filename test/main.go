package main

import (
	"fmt"
	"github.com/kgip/go-rabbit-template"
	uuid "github.com/satori/go.uuid"
	"sync"
	"time"
)

func main() {
	args := []struct {
		exchange   string
		routingKey string
	}{
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
		{"test.rabbit_template.topic.exchange", "test.key"},
		{"test.rabbit_template.topic.exchange", "test.error.key"},
	}

	for i := 0; i < 8; i++ {
		args = append(args, args...)
	}
	template, _ := rabbitmq.NewRabbitTemplate("amqp://guest:guest@192.168.31.41", rabbitmq.Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
	confirmCount := 0
	returnCount := 0
	correlationDataMap := &sync.Map{}
	template.RegisterConfirmCallback(func(ack bool, DeliveryTag uint64, correlationData *rabbitmq.CorrelationData) {
		_, ok := correlationDataMap.Load(correlationData.ID)
		fmt.Println(ack, DeliveryTag, correlationData.ID, ok)
		confirmCount++
	})
	template.RegisterReturnCallback(func(r *rabbitmq.Return) {
		fmt.Println(string(r.Body), r.ReplyText, r.RoutingKey, r.Exchange)
		returnCount++
	})
	for i, arg := range args {
		exchange := arg.exchange
		routingKey := arg.routingKey
		go func(i int, exchange, routingKey string) {
			correlationData := &rabbitmq.CorrelationData{ID: uuid.NewV4().String()}
			err := template.Publish(exchange, routingKey, true, false, &rabbitmq.Message{Body: []byte(fmt.Sprintf("hello---%d", i))}, correlationData)
			if err != nil {
				fmt.Println(err)
			}
			correlationDataMap.Store(correlationData.ID, correlationData)
		}(i, exchange, routingKey)
	}
	time.Sleep(5 * time.Second)
}
