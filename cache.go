package main

import (
	"context"
	"log"
	"sync"
)

type Cache struct {
	orders sync.Map
}

func LoadCache()(error){
	get100LastOrdersQuery := `
	SELECT 
		o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature, 
		o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
		d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
		p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, 
		p.delivery_cost, p.goods_total, p.custom_fee
	FROM orders o
	LEFT JOIN deliveries d ON o.order_uid = d.order_uid
	LEFT JOIN payments p on o.order_uid = p.order_uid

	ORDER BY o.date_created DESC
	LIMIT 100;
	` // одним запросом собираем до 100 заказов
	rows, err := pool.Query(context.TODO(), get100LastOrdersQuery)
	
	if err != nil{
		log.Printf("Не удалось подгрузить кэш: %v", err)
		return err
	}

	var orderUIDs []string // будем собирать все ID сюда, чтобы потом для них извлечь Items одним запросом
	var orders []*Order // а сюда сами заказы, чтобы потом к ним подтянуть их товары
	for rows.Next(){
		order := &Order{
			Delivery: Delivery{},
			Payment: Payment{},
			Items: []Item{},
		}
		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
			&order.CustomerID, &order.DeliveryService, &order.ShardKey, &order.SMID, &order.DateCreated,
			&order.OofShard, &order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip,
			&order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
			&order.Payment.Transaction, &order.Payment.RequestID, &order.Payment.Currency, &order.Payment.Provider, 
			&order.Payment.Amount, &order.Payment.PaymentDT, &order.Payment.Bank, &order.Payment.DeliveryCost, 
			&order.Payment.GoodsTotal, &order.Payment.CustomFee,
		)
		if err != nil{
			log.Printf("Не удалось извлечь заказ: %v", err)
			return err
		}
		orderUIDs = append(orderUIDs, order.OrderUID)
		orders = append(orders, order)
	}
	
	getItemsForOrders := `
		SELECT 
			o_i.order_uid, item.chrt_id, item.track_number, item.price, item.rid, item.name, item.sale,
			item.size, item.total_price, item.nm_id, item.brand, item.status
			FROM items item
			JOIN order_items o_i ON  item.chrt_id = o_i.chrt_id
			WHERE o_i.order_uid = ANY($1);
	`
	rows, err = pool.Query(context.TODO(), getItemsForOrders, orderUIDs)

	if err != nil{
		log.Printf("Не удалось получить товары для заказов из orderUIDs: %v", err)
	}

	orderItems := make(map[string][]Item)

	for rows.Next(){
		var uid string
		var item Item
		err := rows.Scan(&uid, &item.ChrtID, &item.TrackNumber, &item.Price, &item.RID,
						 &item.Name, &item.Sale, &item.Size, &item.TotalPrice, &item.NMID, 
						 &item.Brand, &item.Status,
						)
		if err != nil{
			log.Printf("Не удалось получить товар для заказа: %v", err)
			return err
		}
		orderItems[uid] = append(orderItems[uid], item)

	}
	for _, order := range orders{
		if items, ok := orderItems[order.OrderUID]; ok{
			order.Items = items
		}
		ordersCache.orders.Store(order.OrderUID, order)
	}

	log.Printf("Кэш успешно подгружен: загружено %d заказов", len(orders))
	return nil
}

func GetOrderFromCache(orderUID string) (*Order, bool){
	if value, ok := ordersCache.orders.Load(orderUID); ok{
		if order, ok := value.(*Order); ok{
			return order, true
		}
	}
	return nil, false
}

func SaveOrderToCache(order Order){
	ordersCache.orders.Store(order.OrderUID, order)
	log.Printf("Заказ %v записан в кэш", order.OrderUID)
}

