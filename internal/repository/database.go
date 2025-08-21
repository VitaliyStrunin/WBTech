package repository

import (
	"WBTech/config"
	"WBTech/internal/orders"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	*pgxpool.Pool
}

func NewPostgresRepository(cfg *config.Config) *PostgresRepository {
	connectionString := getPostgresConnectionString(*cfg)
	pool, err := pgxpool.New(context.TODO(), connectionString)

	if err != nil {
		log.Printf("Не удалось подключиться к базе данных: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		log.Printf("Не удалось выполнить пинг базы данных: %v", err)
	}

	return &PostgresRepository{pool}
}

func (postgresRepo *PostgresRepository) GetOrder(orderUID string) (*orders.Order, error) {
	order := &orders.Order{
		Delivery: orders.Delivery{},
		Payment:  orders.Payment{},
		Items:    []orders.Item{},
	}

	var sql = `
		SELECT 
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature, 
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, 
			p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		LEFT JOIN deliveries d ON o.order_uid = d.order_uid
		LEFT JOIN payments p ON o.order_uid = p.order_uid
		WHERE o.order_uid = $1
	`
	err := postgresRepo.QueryRow(context.Background(), sql, orderUID).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
		&order.CustomerID, &order.DeliveryService, &order.ShardKey, &order.SMID, &order.DateCreated, &order.OofShard,
		&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip, &order.Delivery.City,
		&order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
		&order.Payment.Transaction, &order.Payment.RequestID, &order.Payment.Currency,
		&order.Payment.Provider, &order.Payment.Amount, &order.Payment.PaymentDT,
		&order.Payment.Bank, &order.Payment.DeliveryCost, &order.Payment.GoodsTotal,
		&order.Payment.CustomFee,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("заказ %s не найден", orderUID)
	}

	if err != nil {
		return nil, fmt.Errorf("произошла ошибка при загрузки из БД: %v", err)
	}

	items, err := postgresRepo.loadItems(orderUID)
	if err != nil {
		return nil, fmt.Errorf("произошла ошибка загрузки товаров: %v", err)
	}

	order.Items = items
	return order, nil
}

func (postgresRepo *PostgresRepository) SaveOrder(order orders.Order) error {
	transaction, err := postgresRepo.Begin(context.TODO())

	if err != nil {
		return err
	}

	defer func() {
		err := transaction.Rollback(context.TODO())
		if err != nil {
			log.Println("Ошибка отката транзакции")
		}
	}()

	var orderExists bool
	var checkOrderExistanceQuery = `
	SELECT EXISTS(SELECT 1 FROM orders WHERE order_uid = $1)
	`
	err = transaction.QueryRow(context.TODO(), checkOrderExistanceQuery, order.OrderUID).Scan(&orderExists)
	if err != nil {
		return err
	}
	if orderExists {
		return errors.New("заказ уже существует")
	}

	var orderInsertionQuery = `
	INSERT INTO orders(
		order_uid, track_number, entry, locale, internal_signature, 
		customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err = transaction.Exec(context.TODO(), orderInsertionQuery, order.OrderUID, order.TrackNumber,
		order.Entry, order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.ShardKey, order.SMID, order.DateCreated, order.OofShard)
	if err != nil {
		return err
	}

	var deliveryInsertionQuery = `
	INSERT INTO deliveries(
		name, phone, zip, city, address, region, email, order_uid
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = transaction.Exec(context.TODO(), deliveryInsertionQuery, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email, order.OrderUID)
	if err != nil {
		return err
	}

	var paymentInsertionQuery = `
	INSERT INTO payments(
		transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee, order_uid
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err = transaction.Exec(context.TODO(), paymentInsertionQuery, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDT, order.Payment.Bank, order.Payment.DeliveryCost,
		order.Payment.GoodsTotal, order.Payment.CustomFee, order.OrderUID)
	if err != nil {
		return err
	}

	var itemInsertionQuery = `
	INSERT INTO items(
		chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	ON CONFLICT (chrt_id) DO NOTHING;
	`
	var orderItemInsertionQuery = `
		INSERT INTO order_items(
			order_uid, chrt_id
		) VALUES ($1, $2)
	`
	for _, item := range order.Items {

		_, err = transaction.Exec(context.TODO(), itemInsertionQuery, item.ChrtID, item.TrackNumber, item.Price, item.RID, item.Name, item.Sale,
			item.Size, item.TotalPrice, item.NMID, item.Brand, item.Status)
		if err != nil {
			return err
		}

		_, err = transaction.Exec(context.TODO(), orderItemInsertionQuery, order.OrderUID, item.ChrtID)
		if err != nil {
			return err
		}
	}
	return transaction.Commit(context.TODO())
}

func (postgresRepo *PostgresRepository) GetRecentOrders(limit int) ([]*orders.Order, error) {

	var get100LastOrdersQuery = `
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
	LIMIT $1;
	`
	rows, err := postgresRepo.Query(context.TODO(), get100LastOrdersQuery, limit)

	if err != nil {
		log.Printf("Не удалось подгрузить кэш: %v", err)
		return nil, err
	}

	orderUIDs := make([]string, 0, limit) // будем собирать все ID сюда, чтобы потом для них извлечь Items одним запросом
	ordersList := make([]*orders.Order, 0, limit)

	for rows.Next() {
		order := &orders.Order{
			Delivery: orders.Delivery{},
			Payment:  orders.Payment{},
			Items:    []orders.Item{},
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
		if err != nil {
			log.Printf("Не удалось извлечь заказ: %v", err)
			return nil, err
		}
		orderUIDs = append(orderUIDs, order.OrderUID)
		ordersList = append(ordersList, order)
	}

	var getItemsForOrders = `
		SELECT
			o_i.order_uid, item.chrt_id, item.track_number, item.price, item.rid, item.name, item.sale,
			item.size, item.total_price, item.nm_id, item.brand, item.status
			FROM items item
			JOIN order_items o_i ON  item.chrt_id = o_i.chrt_id
			WHERE o_i.order_uid = ANY($1);
	`
	rows, err = postgresRepo.Query(context.TODO(), getItemsForOrders, orderUIDs)

	if err != nil {
		log.Printf("Не удалось получить товары для заказов из orderUIDs: %v", err)
	}

	orderItems := make(map[string][]orders.Item)

	for rows.Next() {
		var uid string
		var item orders.Item
		err := rows.Scan(&uid, &item.ChrtID, &item.TrackNumber, &item.Price, &item.RID,
			&item.Name, &item.Sale, &item.Size, &item.TotalPrice, &item.NMID,
			&item.Brand, &item.Status,
		)
		if err != nil {
			log.Printf("Не удалось получить товар для заказа: %v", err)
			return nil, err
		}
		orderItems[uid] = append(orderItems[uid], item)
	}
	for _, order := range ordersList {
		order.Items = orderItems[order.OrderUID]
	}

	log.Printf("Успешно извлечено %d заказов", len(ordersList))
	return ordersList, nil
}

func (postgresRepo *PostgresRepository) loadItems(orderUID string) ([]orders.Item, error) {
	var sql = `
		SELECT item.chrt_id, item.track_number, item.price, item.rid, item.name, item.sale,
			   item.size, item.total_price, item.nm_id, item.brand, item.status
		FROM items item
		JOIN order_items o_i ON item.chrt_id = o_i.chrt_id
		WHERE o_i.order_uid = $1
	`
	rows, err := postgresRepo.Query(context.Background(), sql, orderUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []orders.Item
	for rows.Next() {
		var item orders.Item
		err := rows.Scan(
			&item.ChrtID, &item.TrackNumber, &item.Price, &item.RID, &item.Name,
			&item.Sale, &item.Size, &item.TotalPrice, &item.NMID, &item.Brand, &item.Status,
		)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, nil
}

func getPostgresConnectionString(cfg config.Config) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.User, cfg.Password, cfg.Host,
		cfg.Port, cfg.Database)
}
