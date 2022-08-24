package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/rabbitmq/amqp091-go"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPoolChannelMax        = 200
	defaultCorrelationDataExpire = 30 * time.Minute

	NoExpire = -1

	// Constants for standard AMQP 0-9-1 exchange types.
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// CorrelationData 每个消息附带的额外信息
type CorrelationData struct {
	ID     string
	Data   interface{}
	Expire time.Duration
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
	PoolChannelMax         uint          //连接池最大channel数
	Timeout                time.Duration //连接获取超时时间，小于0时无超时限制
	CorrelationDataExpire  time.Duration
	Logger                 Logger
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

type Consumer func(delivery *Delivery)

// Message captures the client message sent to the server.  The fields
// outside of the Headers table included in this struct mirror the underlying
// fields in the content frame.  They use native types for convenience and
// efficiency.
type Message struct {
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers map[string]interface{}

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id

	// The application specific payload of the message
	Body []byte
}

type ConfirmCallback func(ack bool, DeliveryTag uint64, correlationData *CorrelationData)

type ReturnCallback func(r *Return)

// RabbitTemplate RabbitMQ操作工具
type RabbitTemplate struct {
	url                  string              //amqp服务器连接地址
	config               *Config             //配置信息
	connection           *amqp091.Connection //生产者连接
	channelPool          *ChannelPool        //信道池，用于根据连接创建信道
	confirmCallback      *atomic.Value       //确认回调
	returnCallback       *atomic.Value       //返回回调
	correlationDataCache *cache.Cache        //存储每个channel每个消息对应的CorrelationData
	consumerCallbacks    []Consumer          //消费者回调
	mutex                *sync.Mutex
	logger               Logger
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
	if int(poolChannelMax) > config.ChannelMax && config.ChannelMax > 0 {
		poolChannelMax = uint(config.ChannelMax)
	}
	if config.Logger == nil {
		config.Logger = &DefaultLogger{
			Level: Info,
			Log:   Zap(),
		}
	}
	pool, err := NewChannelPool(connection, poolChannelMax, config.Timeout, config.Logger)
	if err != nil {
		return nil, err
	}

	correlationDataExpire := config.CorrelationDataExpire
	if correlationDataExpire < 0 {
		correlationDataExpire = defaultCorrelationDataExpire
	} else if correlationDataExpire == 0 {
		correlationDataExpire = cache.NoExpiration
	} else {
		correlationDataExpire = config.CorrelationDataExpire
	}
	return &RabbitTemplate{
		url:                  url,
		connection:           connection,
		channelPool:          pool,
		config:               &config,
		correlationDataCache: cache.New(correlationDataExpire, 2*correlationDataExpire),
		confirmCallback:      &atomic.Value{},
		returnCallback:       &atomic.Value{},
		mutex:                &sync.Mutex{},
		logger:               config.Logger,
	}, nil
}

func (template *RabbitTemplate) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	err = channel.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		template.logger.Error("Failed to create exchange '%s'", name)
	} else {
		template.logger.Info("Create exchange '%s' complete", name)
	}
	return err
}

func (template *RabbitTemplate) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		template.logger.Error("Failed to create queue '%s'", name)
	} else {
		template.logger.Info("Create queue '%s' complete", name)
	}
	return err
}

func (template *RabbitTemplate) QueueBind(name, key, exchange string, noWait bool, args map[string]interface{}) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	err = channel.channel.QueueBind(name, key, exchange, noWait, args)
	if err != nil {
		template.logger.Error("Failed to create queuebind '%s'", exchange+"--"+key+"-->"+name)
	} else {
		template.logger.Info("Create queuebind '%s' complete", exchange+"--"+key+"-->"+name)
	}
	return err
}

func (template *RabbitTemplate) RegisterConfirmCallback(callback ConfirmCallback) {
	template.confirmCallback.Store(callback)
}

func (template *RabbitTemplate) RegisterReturnCallback(callback ReturnCallback) {
	template.returnCallback.Store(callback)
}

func (template *RabbitTemplate) Publish(exchange, key string, mandatory, immediate bool, msg *Message, correlationData *CorrelationData) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if template.config.EnablePublisherConfirm && template.confirmCallback.Load() != nil && channel.confirmChan == nil {
		channel.channel.Confirm(false)
		channel.confirmChan = channel.channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
		//监听确认回调
		go func() {
			for confirmation := range channel.confirmChan {
				cachedCorrelationDataKey := fmt.Sprintf("%v-%d", channel.confirmChan, confirmation.DeliveryTag)
				data, exist := template.correlationDataCache.Get(cachedCorrelationDataKey)
				if exist {
					callback := template.confirmCallback.Load().(ConfirmCallback)
					go callback(confirmation.Ack, confirmation.DeliveryTag, data.(*CorrelationData))
				} else {
					template.logger.Warn(fmt.Sprintf("Lost CorrelationData: channel: %v deliveryTag: %d ack: %v", channel.channel, confirmation.DeliveryTag, confirmation.Ack))
				}
			}
		}()
	}
	if template.config.EnablePublisherReturns && template.returnCallback.Load() != nil && channel.returnChan == nil {
		channel.returnChan = channel.channel.NotifyReturn(make(chan amqp091.Return, 1))
		go func() {
			for r := range channel.returnChan {
				callback := template.returnCallback.Load().(ReturnCallback)
				go callback(&Return{
					ReplyCode:       r.ReplyCode,
					ReplyText:       r.ReplyText,
					Exchange:        r.Exchange,
					RoutingKey:      r.RoutingKey,
					ContentType:     r.ContentType,
					ContentEncoding: r.ContentEncoding,
					Headers:         r.Headers,
					DeliveryMode:    r.DeliveryMode,
					Priority:        r.Priority,
					CorrelationId:   r.CorrelationId,
					ReplyTo:         r.ReplyTo,
					Expiration:      r.Expiration,
					MessageId:       r.MessageId,
					Timestamp:       r.Timestamp,
					Type:            r.Type,
					UserId:          r.UserId,
					AppId:           r.AppId,
					Body:            r.Body,
				})
			}
		}()
	}
	//生成缓存key
	cachedCorrelationDataKey := fmt.Sprintf("%v-%d", channel.confirmChan, channel.channel.GetNextPublishSeqNo())
	//缓存correlationData
	if correlationData.Expire <= 0 {
		template.correlationDataCache.SetDefault(cachedCorrelationDataKey, correlationData)
	} else {
		template.correlationDataCache.Set(cachedCorrelationDataKey, correlationData, correlationData.Expire)
	}
	return channel.channel.PublishWithContext(context.Background(), exchange, key, mandatory, immediate, amqp091.Publishing{
		Headers:         msg.Headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body,
	})
}

func (template *RabbitTemplate) SimplePublish(exchange, key string, data interface{}, correlationData *CorrelationData) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return template.Publish(exchange, key, template.config.EnablePublisherReturns && template.returnCallback.Load() != nil, false, &Message{
		ContentEncoding: "utf-8",
		ContentType:     "application/json",
		Timestamp:       time.Now(),
		DeliveryMode:    amqp091.Persistent,
		Body:            bytes,
	}, correlationData)
}

func (template *RabbitTemplate) RegisterConsumer(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}, consumerHandler Consumer) error {
	channel, err := template.channelPool.GetChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	deliveries, err := channel.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return err
	}
	go func() {
		for delivery := range deliveries {
			template.handleError(consumerHandler, &Delivery{
				Acknowledger:    delivery.Acknowledger,
				Headers:         delivery.Headers,
				ContentType:     delivery.ContentType,
				ContentEncoding: delivery.ContentEncoding,
				DeliveryMode:    delivery.DeliveryMode,
				Priority:        delivery.Priority,
				CorrelationId:   delivery.CorrelationId,
				ReplyTo:         delivery.ReplyTo,
				Expiration:      delivery.Expiration,
				MessageId:       delivery.MessageId,
				Timestamp:       delivery.Timestamp,
				Type:            delivery.Type,
				UserId:          delivery.UserId,
				AppId:           delivery.AppId,
				ConsumerTag:     delivery.ConsumerTag,
				MessageCount:    delivery.MessageCount,
				DeliveryTag:     delivery.DeliveryTag,
				Redelivered:     delivery.Redelivered,
				Exchange:        delivery.Exchange,
				RoutingKey:      delivery.RoutingKey,
				Body:            delivery.Body,
			})
		}
	}()
	return nil
}

func (template *RabbitTemplate) SimpleRegisterConsumer(queue, consumer string, consumerHandler Consumer) error {
	return template.RegisterConsumer(queue, consumer, false, false, false, false, nil, consumerHandler)
}

func (template *RabbitTemplate) handleError(consumerHandler Consumer, delivery *Delivery) {
	defer func() {
		if r := recover(); r != nil {
			template.logger.Warn(fmt.Sprintf("%v", r))
		}
	}()
	consumerHandler(delivery)
}
