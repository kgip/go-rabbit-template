package rabbitmq

import (
	"fmt"
	"testing"
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
