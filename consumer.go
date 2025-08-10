package main

import (
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ConsumerLoop:
		for {
			select{
			case msg := <- partitionConsumer.Messages():
				log.Println("Будем что-то делать с ", msg)

			case <- signals:
				log.Println("Тормозим консьюмер")
				break ConsumerLoop
			}
		}
}

