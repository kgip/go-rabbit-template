# go-rabbit-template

Wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐


reference (https://godoc.org/github.com/wagslane/go-rabbitmq)

## Motivation

[Streadway's AMQP](https://github.com/rabbitmq/amqp091-go) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

## ⚙️ Installation

Inside a Go module:

```bash
go get github.com/kgip/go-rabbit-templa
```

## 🚀 Quick Start Consumer

### Default

```go
template, _ := NewRabbitTemplate(url, Config{})
template.RegisterConsumer("test.rabbit_template.queue", "test.consumer1", false, false, false, false, &Qos{PrefetchCount: 2}, nil, func(delivery *Delivery) {
    fmt.Println(string(delivery.Body), delivery.DeliveryTag, "test.consumer1")
    delivery.Acknowledger.Ack(delivery.DeliveryTag, false)
})
```

### Simple consumer

```go
template, _ := NewRabbitTemplate(url, Config{})
template.SimpleRegisterConsumer("test.rabbit_template.queue", "test.consumer1", func(delivery *Delivery) {
    fmt.Println(string(delivery.Body), delivery.DeliveryTag, "test.consumer1")
    delivery.Acknowledger.Ack(delivery.DeliveryTag, false)
})
```

## 🚀 Quick Start Publisher

### Default

```go
template, _ := NewRabbitTemplate(url, Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
template.RegisterConfirmCallback(func(ack bool, DeliveryTag uint64, correlationData *CorrelationData) {
    fmt.Println(ack, DeliveryTag, correlationData.ID)
})
template.RegisterReturnCallback(func(r *Return) {
    fmt.Println(string(r.Body), r.ReplyText, r.RoutingKey, r.Exchange)
})
correlationData := &CorrelationData{ID: uuid.NewV4().String()}
template.Publish("test.rabbit_template.topic.exchange", "test.key", true, false, &Message{Body: []byte(fmt.Sprintf("hello~"))}, correlationData)
```

### Simple publish

```go
template, _ := NewRabbitTemplate(url, Config{EnablePublisherConfirm: true, EnablePublisherReturns: true})
template.RegisterConfirmCallback(func(ack bool, DeliveryTag uint64, correlationData *CorrelationData) {
    fmt.Println(ack, DeliveryTag, correlationData.ID)
})
template.RegisterReturnCallback(func(r *Return) {
    fmt.Println(string(r.Body), r.ReplyText, r.RoutingKey, r.Exchange)
})
correlationData := &CorrelationData{ID: uuid.NewV4().String()}
template.SimplePublish("test.rabbit_template.topic.exchange", "test.key", "hello~", correlationData)
```

## Other usage examples

See the [test](rabbit_template_test.go) directory for more ideas.