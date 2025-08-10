package main
import (
	"github.com/IBM/sarama"
)


type Producer struct{
	producer sarama.SyncProducer
}

func CreateProducer(broker_addresses []string)(*Producer, error){
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(broker_addresses, producerConfig)
	if err != nil{
		return nil, err
	}

	return &Producer{producer: producer}, nil
}

func (producer *Producer) Produce(topic_name, key string, value []byte) error{
	msg := &sarama.ProducerMessage{
		Topic: topic_name,
		Key: sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err := producer.producer.SendMessage(msg)
	return err
}

