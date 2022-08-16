package rabbit_template

import (
	"errors"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"time"
)

// PoolChannel 连接池包装的信道
type PoolChannel struct {
	channel     *amqp091.Channel  //信道实体,不推荐手动关闭
	isFree      bool              //信道是否空闲
	freeChannel chan *PoolChannel //已经释放的信道会被放入
}

func (channel *PoolChannel) Close() {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
		}
	}()
	channel.isFree = true
	channel.freeChannel <- channel
}

// ChannelPool 信道连接池
type ChannelPool struct {
	conn            *amqp091.Connection
	channels        []*PoolChannel
	maxChannelCount int64 //最大信道数量
	channelCount    int64 //当前信道数量
	mux             *sync.Mutex
	timeout         time.Duration     //获取信道的超时时间
	freeChannel     chan *PoolChannel //已经释放的信道会被放入
	isClosed        bool              //连接池是否关闭
}

func NewChannelPool(conn *amqp091.Connection, maxChannelCount int64, timeout time.Duration) (*ChannelPool, error) {
	if conn == nil {
		return nil, errors.New("connection is nil")
	}
	return &ChannelPool{conn: conn,
		maxChannelCount: maxChannelCount,
		channels:        make([]*PoolChannel, maxChannelCount),
		mux:             &sync.Mutex{},
		timeout:         timeout,
		freeChannel:     make(chan *PoolChannel, maxChannelCount)}, nil
}

// GetChannel 从连接池获取信道
func (pool *ChannelPool) GetChannel() *PoolChannel {
	if pool.isClosed {
		return nil
	}
	//先尝试获取空闲信道，如果获取到了直接返回
	select {
	case freeChannel := <-pool.freeChannel:
		if freeChannel != nil {
			freeChannel.isFree = false
		}
		return freeChannel
	default:
	}
	pool.mux.Lock()
	defer pool.mux.Unlock()
	if pool.isClosed {
		return nil
	}
	//创建的信道数量没有达到最大,创建新的信道
	if pool.maxChannelCount > pool.channelCount {
		//根据连创建一个信道
		originChannel, err := pool.conn.Channel()
		if err != nil {
			log.Println(err)
			return nil
		}
		channel := &PoolChannel{
			channel:     originChannel,
			isFree:      false,
			freeChannel: pool.freeChannel,
		}
		pool.channels[pool.channelCount] = channel
		pool.channelCount++
		return channel
	}
	pool.mux.Unlock()
	//无超时限制
	if pool.timeout < 0 {
		return <-pool.freeChannel
	}
	//从已创建的信道中获取空闲的信道，最多等待30s
	timer := time.NewTimer(pool.timeout)
	for {
		select {
		case freeChannel := <-pool.freeChannel:
			if freeChannel != nil {
				freeChannel.isFree = false
			}
			return freeChannel
		case <-timer.C:
			return nil
		}
	}
}

// Close 关闭连接池
func (pool *ChannelPool) Close() {
	pool.mux.Lock()
	defer pool.mux.Unlock()
	pool.isClosed = true
	close(pool.freeChannel)
	//强制关闭全部信道
	for _, channel := range pool.channels {
		if channel != nil {
			if err := channel.channel.Close(); err != nil {
				log.Println(err)
			}
		}
	}
}
