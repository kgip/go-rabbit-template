package rabbit_template

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
	"sync"
	"testing"
	"time"
)

func TestChannel(t *testing.T) {
	var exchange = "test.exchange"
	var key = "test"
	//var queue = "test.queue"
	connection, _ := amqp091.DialConfig("amqp://guest:guest@192.168.31.24", amqp091.Config{Heartbeat: 60 * time.Second})
	pool, _ := NewChannelPool(connection, 100, 10*time.Second)
	wg := &sync.WaitGroup{}
	wg.Add(5000)
	for i := 0; i < 5000; i++ {
		go func() {
			channel, _ := pool.GetChannel()
			if channel != nil {
				channel.channel.PublishWithContext(context.Background(),
					exchange, key, true, false,
					amqp091.Publishing{CorrelationId: uuid.NewV4().String(), Body: []byte("hahaha")})
				time.Sleep(200 * time.Millisecond)
				channel.Close()
			}
			wg.Done()
		}()
		time.Sleep(time.Millisecond * 10)
	}
	wg.Wait()
	//for {
	//	fmt.Println("aaa")
	//	time.Sleep(time.Second)
	//}
	pool.Close()
}
