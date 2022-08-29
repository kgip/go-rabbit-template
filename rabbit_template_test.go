package rabbitmq

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRabbitTemplate_ExchangeDeclare(t *testing.T) {
	args := []struct {
		exchangeType string
	}{{ExchangeDirect}, {ExchangeFanout}, {ExchangeHeaders}, {ExchangeTopic}}

	template, _ := NewRabbitTemplate("amqp://guest:guest@192.168.31.41", Config{})
	for _, arg := range args {
		err := template.ExchangeDeclare(fmt.Sprintf("test.rabbit_template.%s.exchange", arg.exchangeType), arg.exchangeType, true, false, false, false, nil)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestRabbitTemplate_QueueDeclare(t *testing.T) {
	template, _ := NewRabbitTemplate("amqp://guest:guest@192.168.31.41", Config{})
	err := template.QueueDeclare("test.rabbit_template.queue", true, false, false, false, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestRabbitTemplate_QueueBind(t *testing.T) {
	args := []struct {
		exchangeType string
	}{{ExchangeDirect}, {ExchangeFanout}, {ExchangeHeaders}, {ExchangeTopic}}

	template, _ := NewRabbitTemplate("amqp://guest:guest@192.168.31.41", Config{})
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
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
		//{"test.rabbit_template.topic.exchange", "test.key"},
		//{"test.rabbit_template.topic.exchange", "test.error.key"},
	}

	template, _ := NewRabbitTemplate("amqp://guest:guest@192.168.31.41", Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
	confirmCount := 0
	returnCount := 0
	template.RegisterConfirmCallback(func(ack bool, DeliveryTag uint64, correlationData *CorrelationData) {
		t.Log(ack, DeliveryTag, correlationData.ID)
		confirmCount++
	})
	template.RegisterReturnCallback(func(r *Return) {
		t.Log(string(r.Body), r.ReplyText, r.RoutingKey, r.Exchange)
		returnCount++
	})

	for i, arg := range args {
		go func(i int) {
			err := template.Publish(arg.exchange, arg.routingKey, true, false, &Message{Body: []byte(fmt.Sprintf("hello---%d", i))}, &CorrelationData{ID: uuid.NewV4().String()})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	time.Sleep(2 * time.Second)
	assert.Equal(t, 16, confirmCount)
	assert.Equal(t, 8, returnCount)
}
