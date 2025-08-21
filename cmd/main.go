package main

import (
	"log"
	"net/http"

	"WBTech/config"
	"WBTech/internal/consumer"
	"WBTech/internal/handlers"
	"WBTech/internal/repository"
	"WBTech/internal/service"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	cfg := config.NewConfig()
	ordersDatabase := repository.NewPostgresRepository(cfg)
	ordersCache := repository.NewCache()
	ordersService := service.NewOrderService(ordersDatabase, ordersCache)
	defer ordersDatabase.Close()

	err := ordersService.LoadCache(100)

	if err != nil {
		log.Printf("Не удалось загрузить кэш: %v", err)
	} else {
		log.Printf("Кэш успешно подгружен")
	}
	go func() {
		kafkaConsumer, err := consumer.CreateConsumer(*cfg, ordersService)
		if err != nil {
			log.Fatalf("Ошибка при создании консьюмера: %v", err)
		}
		kafkaConsumer.Consume(*cfg)
	}()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		http.ServeFile(writer, request, "web/index.html")
	})

	http.HandleFunc("/order/", handlers.NewOrderHandler(ordersService).HandleOrderRequest)

	log.Println("Запускаемся на localhost:8000")
	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Printf("Ошибка запуска сервера: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}
