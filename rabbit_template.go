package rabbit_template

import (
	"crypto/tls"
	"github.com/rabbitmq/amqp091-go"
	"net"
	"sync"
	"time"
)

const (
	defaultPoolChannelMax = 200

	// Constants for standard AMQP 0-9-1 exchange types.
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// CorrelationData 每个消息附带的额外信息
type CorrelationData struct {
	ID   string
	Data interface{}
}

// Authentication interface provides a means for different SASL authentication
// mechanisms to be used during connection tuning.
type Authentication interface {
	Mechanism() string
	Response() string
}

type AuthenticationProxy struct {
	auth amqp091.Authentication
}

func (auth *AuthenticationProxy) Mechanism() string {
	return auth.auth.Mechanism()
}

func (auth *AuthenticationProxy) Response() string {
	return auth.auth.Response()
}

type Config struct {
	// The SASL mechanisms to try in the client request, and the successful
	// mechanism used on the Connection object.
	// If SASL is nil, PlainAuth from the URL is used.
	SASL []Authentication

	// Vhost specifies the namespace of permissions, exchanges, queues and
	// bindings on the server.  Dial sets this to the path parsed from the URL.
	Vhost string

	ChannelMax int           // 0 max channels means 2^16 - 1
	FrameSize  int           // 0 max bytes means unlimited
	Heartbeat  time.Duration // less than 1s uses the server's interval

	// TLSClientConfig specifies the client configuration of the TLS connection
	// when establishing a tls transport.
	// If the URL uses an amqps scheme, then an empty tls.Config with the
	// ServerName from the URL is used.
	TLSClientConfig *tls.Config

	// Properties is table of properties that the client advertises to the server.
	// This is an optional setting - if the application does not set this,
	// the underlying library will use a generic set of client properties.
	Properties map[string]interface{}

	// Connection locale that we expect to always be en_US
	// Even though servers must return it as per the AMQP 0-9-1 spec,
	// we are not aware of it being used other than to satisfy the spec requirements
	Locale string

	// Dial returns a net.Conn prepared for a TLS handshake with TSLClientConfig,
	// then an AMQP connection handshake.
	// If Dial is nil, net.DialTimeout with a 30s connection and 30s deadline is
	// used during TLS and AMQP handshaking.
	Dial func(network, addr string) (net.Conn, error)

	//channel pool config
	EnablePublisherConfirm bool
	EnablePublisherReturns bool
	PoolChannelMax         int           //连接池最大channel数
	Timeout                time.Duration //连接获取超时时间，小于0时无超时限制
}

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	ReplyCode  uint16 // reason
	ReplyText  string // description
	Exchange   string // basic.publish exchange
	RoutingKey string // basic.publish routing key

	// Properties
	ContentType     string                 // MIME content type
	ContentEncoding string                 // MIME content encoding
	Headers         map[string]interface{} // Application or header exchange table
	DeliveryMode    uint8                  // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8                  // queue implementation use - 0 to 9
	CorrelationId   string                 // application use - correlation identifier
	ReplyTo         string                 // application use - address to to reply to (ex: RPC)
	Expiration      string                 // implementation use - message expiration spec
	MessageId       string                 // application use - message identifier
	Timestamp       time.Time              // application use - message timestamp
	Type            string                 // application use - message type name
	UserId          string                 // application use - creating user id
	AppId           string                 // application use - creating application

	Body []byte
}

// AcknowledgerAdapter notifies the server of successful or failed consumption of
// deliveries via identifier found in the Delivery.DeliveryTag field.
// Applications can provide mock implementations in tests of Delivery handlers.
type AcknowledgerAdapter interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
}

type Acknowledger struct {
	acknowledger amqp091.Acknowledger
}

func (acknowledger *Acknowledger) Ack(tag uint64, multiple bool) error {
	return acknowledger.acknowledger.Ack(tag, multiple)
}

func (acknowledger *Acknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	return acknowledger.acknowledger.Nack(tag, multiple, requeue)
}

func (acknowledger *Acknowledger) Reject(tag uint64, requeue bool) error {
	return acknowledger.acknowledger.Reject(tag, requeue)
}

// Delivery captures the fields for a previously delivered message resident in
// a queue to be delivered by the server to a consumer from Channel.Consume or
// Channel.Get.
type Delivery struct {
	Acknowledger AcknowledgerAdapter // the channel from which this delivery arrived

	Headers map[string]interface{} // Application or header exchange table

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8     // queue implementation use - 0 to 9
	CorrelationId   string    // application use - correlation identifier
	ReplyTo         string    // application use - address to reply to (ex: RPC)
	Expiration      string    // implementation use - message expiration spec
	MessageId       string    // application use - message identifier
	Timestamp       time.Time // application use - message timestamp
	Type            string    // application use - message type name
	UserId          string    // application use - creating user - should be authenticated user
	AppId           string    // application use - creating application id

	// Valid only with Channel.Consume
	ConsumerTag string

	// Valid only with Channel.Get
	MessageCount uint32

	DeliveryTag uint64
	Redelivered bool
	Exchange    string // basic.publish exchange
	RoutingKey  string // basic.publish routing key

	Body []byte
}

// RabbitTemplate RabbitMQ操作工具
type RabbitTemplate struct {
	url                string                                                              //amqp服务器连接地址
	config             *Config                                                             //配置信息
	connection         *amqp091.Connection                                                 //生产者连接
	channelPool        *ChannelPool                                                        //信道池，用于根据连接创建信道
	confirmCallback    func(ack bool, DeliveryTag int64, correlationData *CorrelationData) //确认回调
	returnCallback     func(r *Return)                                                     //返回回调
	correlationDataMap *sync.Map                                                           //存储每个channel每个消息对应的CorrelationData
	consumerCallbacks  []func(delivery *Delivery)                                          //消费者回调
}

func NewRabbitTemplate(url string, config Config) (*RabbitTemplate, error) {
	var SASLs []amqp091.Authentication
	if len(config.SASL) > 0 {
		SASLs = make([]amqp091.Authentication, len(config.SASL))
		for i, auth := range config.SASL {
			SASLs[i] = &AuthenticationProxy{auth: auth}
		}
	}
	connection, err := amqp091.DialConfig(url, amqp091.Config{
		SASL:            SASLs,
		Vhost:           config.Vhost,
		ChannelMax:      config.ChannelMax,
		FrameSize:       config.FrameSize,
		Heartbeat:       config.Heartbeat,
		TLSClientConfig: config.TLSClientConfig,
		Properties:      config.Properties,
		Locale:          config.Locale,
		Dial:            config.Dial,
	})
	if err != nil {
		return nil, err
	}
	var poolChannelMax = config.PoolChannelMax
	if config.PoolChannelMax <= 0 {
		poolChannelMax = defaultPoolChannelMax
	}
	pool, err := NewChannelPool(connection, poolChannelMax, config.Timeout)

	if err != nil {
		return nil, err
	}

	return &RabbitTemplate{
		url:                url,
		connection:         connection,
		channelPool:        pool,
		config:             &config,
		correlationDataMap: &sync.Map{},
	}, nil
}

func (template *RabbitTemplate) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (template *RabbitTemplate) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	return err
}

func (template *RabbitTemplate) QueueBind(name, key, exchange string, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return channel.channel.QueueBind(name, key, exchange, noWait, args)
}
