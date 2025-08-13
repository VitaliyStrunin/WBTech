package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)



var ordersCache Cache

func main() {

	InitDatabase()
	err := LoadCache()

	if err != nil{
		log.Printf("Кэш не подгружен: %v", err)
	}

	go func(){
		consumer, err := CreateConsumer([]string{"localhost:9092"})
		if err != nil{
			log.Fatalf("Ошибка при создании консьюмера: %v", err)
		}
		consumer.Consume("orders")

	}()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request){
		http.ServeFile(writer, request, "index.html")
	})
	
	http.HandleFunc("/order/", HandleOrderRequest)

	log.Println("Запускаемся на localhost:8000")
	go func(){
		if err := http.ListenAndServe(":8000", nil); err != nil{
			log.Printf("Ошибка запуска сервера: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<- stop

	
}

