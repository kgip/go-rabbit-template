package rabbitmq

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

var url = "amqp://guest:guest@192.168.31.41"

func TestRabbitTemplate_ExchangeDeclare(t *testing.T) {
	args := []struct {
		exchangeType string
	}{{ExchangeDirect}, {ExchangeFanout}, {ExchangeHeaders}, {ExchangeTopic}}

	template, _ := NewRabbitTemplate(url, Config{})
	for _, arg := range args {
		err := template.ExchangeDeclare(fmt.Sprintf("test.rabbit_template.%s.exchange", arg.exchangeType), arg.exchangeType, true, false, false, false, nil)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestRabbitTemplate_QueueDeclare(t *testing.T) {
	template, _ := NewRabbitTemplate(url, Config{})
	err := template.QueueDeclare("test.rabbit_template.queue", true, false, false, false, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestRabbitTemplate_QueueBind(t *testing.T) {
	args := []struct {
		exchangeType string
	}{{ExchangeDirect}, {ExchangeFanout}, {ExchangeHeaders}, {ExchangeTopic}}

	template, _ := NewRabbitTemplate(url, Config{})
	for _, arg := range args {
		err := template.QueueBind("test.rabbit_template.queue", "test.key", fmt.Sprintf("test.rabbit_template.%s.exchange", arg.exchangeType), false, nil)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestRabbitTemplate_Publish(t *testing.T) {
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

	template, _ := NewRabbitTemplate(url, Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
	confirmCount := 0
	returnCount := 0
	correlationDataMap := &sync.Map{}
	template.RegisterConfirmCallback(func(ack bool, DeliveryTag uint64, correlationData *CorrelationData) {
		_, ok := correlationDataMap.Load(correlationData.ID)
		t.Log(ack, DeliveryTag, correlationData.ID, ok)
		confirmCount++
	})
	template.RegisterReturnCallback(func(r *Return) {
		t.Log(string(r.Body), r.ReplyText, r.RoutingKey, r.Exchange)
		returnCount++
	})
	for i, arg := range args {
		exchange := arg.exchange
		routingKey := arg.routingKey
		go func(i int, exchange, routingKey string) {
			correlationData := &CorrelationData{ID: uuid.NewV4().String()}
			err := template.Publish(exchange, routingKey, true, false, &Message{Body: []byte(fmt.Sprintf("hello---%d", i))}, correlationData)
			if err != nil {
				t.Error(err)
			}
			correlationDataMap.Store(correlationData.ID, correlationData)
		}(i, exchange, routingKey)
	}
	time.Sleep(time.Second)
	assert.Equal(t, len(args), confirmCount)
	assert.Equal(t, len(args)/2, returnCount)
}

func TestRabbitTemplate_RegisterConsumer(t *testing.T) {
	template, _ := NewRabbitTemplate(url, Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
	template.RegisterConsumer("test.rabbit_template.queue", "test.consumer1", false, false, false, false, &Qos{PrefetchCount: 2}, nil, func(delivery *Delivery) {
		t.Log(string(delivery.Body), delivery.DeliveryTag, "test.consumer1")
		time.Sleep(time.Second)
		delivery.Acknowledger.Ack(delivery.DeliveryTag, false)
	})
	template.RegisterConsumer("test.rabbit_template.queue", "test.consumer2", false, false, false, false, &Qos{PrefetchCount: 2}, nil, func(delivery *Delivery) {
		t.Log(string(delivery.Body), delivery.DeliveryTag, "test.consumer2")
		time.Sleep(time.Second)
		delivery.Acknowledger.Ack(delivery.DeliveryTag, false)
	})
	time.Sleep(10 * time.Second)
}
