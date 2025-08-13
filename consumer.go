package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Consumer struct{
	consumer sarama.Consumer
}


func CreateConsumer(broker_addresses []string)(*Consumer, error){
	consumer, err := sarama.NewConsumer(broker_addresses, nil)
	if err != nil{
		return nil, err
	}
	return &Consumer{consumer: consumer}, nil

}

func (consumer Consumer) Consume(topic_name string){
	partitionConsumer, err := consumer.consumer.ConsumePartition(topic_name, 0, sarama.OffsetNewest)
	if err != nil{
		log.Fatalf("Произошла ошибка подписки на топик: %v", err)
	}
	defer partitionConsumer.Close() 
	log.Println("Консьюмер создан, запущен и ожидает сообщения из топика orders")
	
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ConsumerLoop:
		for {
			select{
			case msg := <- partitionConsumer.Messages():
				var order Order
				err = json.Unmarshal(msg.Value, &order)
				if err != nil{
					log.Printf("Не получилось распарсить информацию о заказе: %v", err)
					continue
				}
				
				err := SaveOrderToDatabase(order)
				if err != nil{
					log.Printf("Не удалось сохранить заказ в базу данных: %v", err)
					continue
				}
				SaveOrderToCache(order)

				log.Printf("Заказ %v успешно сохранён!", order.OrderUID)

			case <- signals:
				log.Println("Тормозим консьюмер")
				break ConsumerLoop
			}
		}
}

