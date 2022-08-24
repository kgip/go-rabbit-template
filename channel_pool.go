package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"time"
)

const (
	defaultTimeout = 10 * time.Second
)

// PoolChannel 连接池包装的信道
type PoolChannel struct {
	channel     *amqp091.Channel  //信道实体,不推荐手动关闭
	isFree      bool              //信道是否空闲
	freeChannel chan *PoolChannel //已经释放的信道会被放入
	confirmChan chan amqp091.Confirmation
	returnChan  chan amqp091.Return
	mutex       *sync.Mutex
}

func (channel *PoolChannel) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
			if er, ok := r.(error); ok {
				err = er
			} else {
				err = errors.New(fmt.Sprintf("failed to close channel %v", channel))
			}
		}
	}()
	channel.isFree = true
	channel.freeChannel <- channel
	return
}

// ChannelPool 信道连接池
type ChannelPool struct {
	conn            *amqp091.Connection
	channels        []*PoolChannel
	maxChannelCount int //最大信道数量
	channelCount    int //当前信道数量
	mux             *sync.Mutex
	timeout         time.Duration     //获取信道的超时时间
	freeChannel     chan *PoolChannel //已经释放的信道会被放入
	isClosed        bool              //连接池是否关闭
	logger          Logger
}

// NewChannelPool 创建一个连接池
// conn amqp连接
// maxChannelCount 连接池最大连接数
// timeout 获取连接的
func NewChannelPool(conn *amqp091.Connection, maxChannelCount int, timeout time.Duration, logger Logger) (*ChannelPool, error) {
	if conn == nil {
		return nil, errors.New("'conn' is nil")
	}
	if maxChannelCount <= 0 {
		return nil, errors.New("'maxChannelCount' is less than 0")
	}
	if timeout == 0 {
		timeout = defaultTimeout
	}
	if logger == nil {
		logger = &DefaultLogger{
			Level: Info,
			Log:   Zap(),
		}
	}
	return &ChannelPool{conn: conn,
		maxChannelCount: maxChannelCount,
		channels:        make([]*PoolChannel, maxChannelCount),
		mux:             &sync.Mutex{},
		timeout:         timeout,
		freeChannel:     make(chan *PoolChannel, maxChannelCount),
		logger:          logger}, nil
}

// GetChannel 从连接池获取信道
func (pool *ChannelPool) GetChannel() (ch *PoolChannel, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
			if er, ok := r.(error); ok {
				err = er
			} else {
				err = errors.New("failed to acquire channel")
			}
		}
	}()
	if pool.isClosed {
		return nil, errors.New("pool is closed")
	}
	//先尝试获取空闲信道，如果获取到了直接返回
	select {
	case freeChannel := <-pool.freeChannel:
		if freeChannel != nil && !freeChannel.channel.IsClosed() {
			freeChannel.isFree = false
			return freeChannel, nil
		}
	default:
	}

	channel, err := pool.createNewChannel()
	if err != nil {
		return nil, err
	} else if channel != nil {
		return channel, nil
	}
	//无超时限制
	if pool.timeout < 0 {
		return <-pool.freeChannel, err
	}
	//从已创建的信道中获取空闲的信道，最多等待30s
	timer := time.NewTimer(pool.timeout)
	for {
		select {
		case freeChannel := <-pool.freeChannel:
			if freeChannel != nil && !freeChannel.channel.IsClosed() {
				freeChannel.isFree = false
				return freeChannel, nil
			}
		case <-timer.C:
			return nil, errors.New("acquire channel timeout")
		}
	}
}

func (pool *ChannelPool) createNewChannel() (ch *PoolChannel, err error) {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	if pool.isClosed {
		return nil, errors.New("pool is closed")
	}
	//创建的信道数量没有达到最大,创建新的信道
	if pool.maxChannelCount > pool.channelCount {
		var originChannel *amqp091.Channel
		//根据连创建一个信道
		originChannel, err = pool.conn.Channel()
		if err != nil {
			pool.logger.Error(err.Error())
			return nil, err
		}
		channel := &PoolChannel{
			channel:     originChannel,
			isFree:      false,
			freeChannel: pool.freeChannel,
			mutex:       &sync.Mutex{},
		}
		pool.channels[pool.channelCount] = channel
		pool.channelCount++
		return channel, nil
	}
	return nil, nil
}

// Close 关闭连接池
func (pool *ChannelPool) Close() (err error) {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	pool.isClosed = true
	close(pool.freeChannel)
	//强制关闭全部信道
	for _, channel := range pool.channels {
		if channel != nil {
			if e := channel.channel.Close(); e != nil {
				pool.logger.Error(e.Error())
			}
		}
	}
	return
}
