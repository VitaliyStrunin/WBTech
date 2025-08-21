package handlers

import (
	"WBTech/internal/orders"
	"WBTech/internal/service"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

type OrderHandler struct {
	service *service.OrderService
}

func NewOrderHandler(service *service.OrderService) *OrderHandler {
	return &OrderHandler{service}
}

func (handler *OrderHandler) HandleOrderRequest(writer http.ResponseWriter, request *http.Request) {
	urlPath := request.URL.Path
	orderUID := ExtractOrderUID(urlPath)

	if orderUID == "" {
		http.Error(writer, "Укажите корректный OrderUID: например, /orders/test123", http.StatusBadRequest)
		return
	}

	if order, err := handler.service.GetOrder(orderUID); err == nil {
		SendOrderJSON(writer, order)
		return
	} else {
		http.Error(writer, "Не удалось найти заказ", http.StatusNotFound)
	}

}

func ExtractOrderUID(path string) string {
	pathParts := strings.Split(path, "/")
	return pathParts[len(pathParts)-1]
}

func SendOrderJSON(writer http.ResponseWriter, order *orders.Order) {
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(order); err != nil {
		log.Printf("Произошла ошибкка сериализации заказа: %v", err)
		http.Error(writer, "Ошибка данных", http.StatusInternalServerError)
	}
}
