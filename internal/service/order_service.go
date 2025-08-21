package service

import (
	"WBTech/internal/orders"
	"WBTech/internal/repository"
)

type OrderService struct {
	repo  repository.OrderRepository
	cache repository.CacheRepository
}

func NewOrderService(repo repository.OrderRepository, cache repository.CacheRepository) *OrderService {
	return &OrderService{repo: repo, cache: cache}
}

func (service *OrderService) LoadCache(limit int) error {
	ordersList, err := service.repo.GetRecentOrders(limit)
	if err != nil {
		return err
	}

	for _, order := range ordersList {
		err := service.cache.Save(order)
		if err != nil {
			return err
		}
	}
	return nil
}

func (service *OrderService) SaveOrder(order *orders.Order) error {
	if err := service.repo.SaveOrder(*order); err != nil {
		return err
	}
	return service.cache.Save(order)
}

func (service *OrderService) GetOrder(orderUID string) (*orders.Order, error) {
	if order, ok := service.cache.Get(orderUID); ok {
		return order, nil
	}

	order, err := service.repo.GetOrder(orderUID)
	if err != nil {
		return nil, err
	}
	return order, nil
}

func (service *OrderService) UpdateCache(order *orders.Order) error {
	err := service.cache.Save(order)
	if err != nil {
		return err
	}
	return nil
}
