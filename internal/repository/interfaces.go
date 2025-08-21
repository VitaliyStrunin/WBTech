package repository

import "WBTech/internal/orders"

type OrderRepository interface {
	GetOrder(orderUID string) (*orders.Order, error)
	SaveOrder(order orders.Order) error
	GetRecentOrders(limit int) ([]*orders.Order, error)
}

type CacheRepository interface {
	Get(orderUID string) (*orders.Order, bool)
	Save(order *orders.Order) error
}
