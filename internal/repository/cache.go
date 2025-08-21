package repository

import (
	"WBTech/internal/orders"
	"errors"
	"fmt"
	"sync"
)

type Cache struct {
	sync.RWMutex
	orders map[string]*orders.Order
}

func NewCache() *Cache {
	return &Cache{orders: make(map[string]*orders.Order)}
}

func (cache *Cache) Get(orderUID string) (*orders.Order, bool) {
	cache.RLock()
	defer cache.RUnlock()
	if order, ok := cache.orders[orderUID]; ok {
		return order, true
	}
	return nil, false
}

func (cache *Cache) Save(order *orders.Order) error {
	cache.Lock()
	defer cache.Unlock()
	if _, ok := cache.orders[order.OrderUID]; !ok {
		cache.orders[order.OrderUID] = order
		return nil
	}
	return errors.New(fmt.Sprintf("%v уже существует в кэше", order.OrderUID))
}
