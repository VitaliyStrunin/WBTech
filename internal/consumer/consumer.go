package consumer

import (
	"WBTech/config"
	"WBTech/internal/orders"
	"WBTech/internal/service"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Consumer struct {
	consumer sarama.Consumer
	service  *service.OrderService
}

func CreateConsumer(connectionConfig config.Config, orderService *service.OrderService) (*Consumer, error) {
	consumer, err := sarama.NewConsumer(connectionConfig.Brokers, nil)
	if err != nil {
		return nil, err
	}
	return &Consumer{consumer: consumer, service: orderService}, nil

}

func (consumer Consumer) Consume(connectionConfig config.Config) {
	partitionConsumer, err := consumer.consumer.ConsumePartition(connectionConfig.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Произошла ошибка подписки на топик: %v", err)
	}
	defer partitionConsumer.AsyncClose()
	log.Println("Консьюмер создан, запущен и ожидает сообщения из топика orders")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var order orders.Order
			err = json.Unmarshal(msg.Value, &order)
			if err != nil {
				log.Printf("Не получилось распарсить информацию о заказе: %v", err)
				continue
			}

			err := consumer.service.SaveOrder(&order)
			if err != nil {
				log.Printf("Не удалось сохранить заказ в базу данных: %v", err)
				continue
			}

			err = consumer.service.UpdateCache(&order)
			if err != nil {
				log.Printf("Не удалось сохранить заказ в кэш: %v", err)
			} else {
				log.Printf("Заказ %v успешно сохранён!", order.OrderUID)
			}

		case <-signals:
			log.Println("Тормозим консьюмер")
			break ConsumerLoop
		}
	}
}
