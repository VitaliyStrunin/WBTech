package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)


func HandleOrderRequest(writer http.ResponseWriter, request *http.Request){
	url_path := request.URL.Path
	orderUID := ExtractOrderUID(url_path)

	if orderUID == ""{
		http.Error(writer, "Укажите корректный OrderUID: например, /order/test123", http.StatusBadRequest)
		return
	}

	if order, ok := GetOrderFromCache(orderUID); ok{
		SendOrderJSON(writer, order)
		return
	}

	order, err := GetOrderFromDatabase(orderUID)

	if order == nil{
		http.Error(writer, "Заказ с указанным UID отсутствует", http.StatusBadRequest)
		return
	}

	if err != nil{
		log.Printf("Ошибка извлечения заказа %v: %v", orderUID, err)
		http.Error(writer, "Заказ с указанным UID отсутствует", http.StatusBadRequest)
		return
	}
	SendOrderJSON(writer, order)
}

func ExtractOrderUID(path string) string{
	path_parts := strings.Split(path, "/")
	return path_parts[len(path_parts) - 1]
}

func GetOrderFromCache(orderUID string) (*Order, bool){
	if value, ok := ordersCache.orders.Load(orderUID); ok{
		if order, ok := value.(*Order); ok{
			return order, true
		}
	}
	return nil, false
}


func SendOrderJSON(writer http.ResponseWriter, order *Order){
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(order); err != nil{
		log.Printf("Произошла ошибкка сериализации заказа: %v", err)
		http.Error(writer, "Ошибка данных", http.StatusInternalServerError)
	}
}