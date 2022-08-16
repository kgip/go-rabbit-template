package rabbit_template

import (
	"github.com/rabbitmq/amqp091-go"
)

// RabbitTemplate RabbitMQ操作工具
type RabbitTemplate struct {
	publisherConn *amqp091.Connection //生产者连接
	consumerConn  *amqp091.Connection //消费者连接
	channelPool   *PoolChannel        //信道池，用于根据连接创建信道
}
