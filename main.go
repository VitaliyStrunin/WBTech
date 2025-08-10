package main

import (
	"log"
	"sync"
	"net/http"
)

type Cache struct{
	orders sync.Map
}

var ordersCache Cache

func main() {

	InitDatabase()
	err := LoadCacheFromDatabase()

	if err != nil{
		log.Printf("Кэш не подгружен: %v", err)
	}

	log.Println("Запускаемся на localhost:8000")

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request){
		http.ServeFile(writer, request, "index.html")
	})
	
	http.HandleFunc("/order/", HandleOrderRequest)
	
	if err := http.ListenAndServe(":8000", nil); err != nil{
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}

	
}

