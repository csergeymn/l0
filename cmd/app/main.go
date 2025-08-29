package main

import (
	"L_0/internal/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// ---------------------------- Globals ----------------------------

var (
	orderCache   = make(map[string]*models.Order)
	orderCacheMu sync.RWMutex
	dbPool       *pgxpool.Pool
)

// ---------------------------- Main ----------------------------

func main() {
	ctx := context.Background()

	// Connect DB
	var err error
	dbPool, err = pgxpool.New(ctx,
		"postgres://postgres:postgres@localhost:5432/go_learning?sslmode=disable")
	if err != nil {
		log.Fatalf("DB connection failed: %v", err)
	}
	defer dbPool.Close()

	// Init cache
	if err := initCache(ctx); err != nil {
		log.Fatalf("Init cache error: %v", err)
	}

	// Start Kafka consumer
	go consumeKafka(ctx)

	// Routes
	http.HandleFunc("GET /order/{uid}/", orderShowByUid)
	http.HandleFunc("/", indexPageRender)

	fmt.Println("🚀 Server running on http://localhost:8080")
	if err := http.ListenAndServe("0.0.0.0:8080", nil); err != nil {
		log.Fatal(err)
	}
}

// ---------------------------- DB & Cache ----------------------------

func initCache(ctx context.Context) error {
	rows, err := dbPool.Query(ctx, `
		SELECT 
			o.order_uid, o.track_number, o.entry, o.locale, 
			o.internal_signature, o.customer_id, o.delivery_service, 
			o.shardkey, o.sm_id, o.date_created, o.oof_shard,

			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,

			p.transaction, p.request_id, p.currency, p.provider, 
			p.amount, p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		JOIN deliveries d ON o.order_uid = d.order_uid
		JOIN payments p ON o.order_uid = p.order_uid
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var order models.Order
		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale,
			&order.InternalSignature, &order.CustomerID, &order.DeliveryService,
			&order.ShardKey, &order.SmID, &order.DateCreated, &order.OofShard,
			&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip,
			&order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
			&order.Payment.Transaction, &order.Payment.RequestID, &order.Payment.Currency,
			&order.Payment.Provider, &order.Payment.Amount, &order.Payment.PaymentDT,
			&order.Payment.Bank, &order.Payment.DeliveryCost, &order.Payment.GoodsTotal, &order.Payment.CustomFee,
		)
		if err != nil {
			return err
		}

		// Fetch items
		itemRows, err := dbPool.Query(ctx, `
			SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
			FROM items WHERE order_uid = $1
		`, order.OrderUID)
		if err != nil {
			return err
		}
		defer itemRows.Close()

		for itemRows.Next() {
			var it models.Item
			if err := itemRows.Scan(
				&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID,
				&it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID,
				&it.Brand, &it.Status,
			); err != nil {
				return err
			}
			order.Items = append(order.Items, it)
		}

		orderCacheMu.Lock()
		orderCache[order.OrderUID] = &order
		orderCacheMu.Unlock()
	}

	log.Printf("✅ Cache initialized with %d orders\n", len(orderCache))
	return nil
}

// ---------------------------- HTTP ----------------------------

func indexPageRender(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

func orderShowByUid(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	orderUid := r.PathValue("uid")
	if orderUid == "" {
		http.Error(w, "Invalid or missing order_id", http.StatusBadRequest)
		return
	}

	order, err := getOrderByUID(r.Context(), orderUid)
	if err != nil {
		http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if order == nil {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(order)
}

func getOrderByUID(ctx context.Context, uid string) (*models.Order, error) {
	// cache
	orderCacheMu.RLock()
	if cached, ok := orderCache[uid]; ok {
		orderCacheMu.RUnlock()
		fmt.Println("⚡ Cache hit for", uid)
		return cached, nil
	}
	orderCacheMu.RUnlock()

	var order models.Order
	err := dbPool.QueryRow(ctx, `
		SELECT order_uid, track_number, entry, locale, internal_signature, customer_id,
		       delivery_service, shardkey, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid = $1
	`, uid).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale,
		&order.InternalSignature, &order.CustomerID, &order.DeliveryService,
		&order.ShardKey, &order.SmID, &order.DateCreated, &order.OofShard,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	// delivery
	err = dbPool.QueryRow(ctx, `
		SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid = $1
	`, uid).Scan(
		&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip,
		&order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region,
		&order.Delivery.Email,
	)
	if err != nil {
		return nil, err
	}

	// payment
	err = dbPool.QueryRow(ctx, `
		SELECT transaction, request_id, currency, provider, amount, payment_dt,
		       bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid = $1
	`, uid).Scan(
		&order.Payment.Transaction, &order.Payment.RequestID, &order.Payment.Currency,
		&order.Payment.Provider, &order.Payment.Amount, &order.Payment.PaymentDT,
		&order.Payment.Bank, &order.Payment.DeliveryCost, &order.Payment.GoodsTotal, &order.Payment.CustomFee,
	)
	if err != nil {
		return nil, err
	}

	// items
	rows, err := dbPool.Query(ctx, `
		SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = $1
	`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var it models.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID,
			&it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID,
			&it.Brand, &it.Status); err != nil {
			return nil, err
		}
		order.Items = append(order.Items, it)
	}

	orderCacheMu.Lock()
	orderCache[uid] = &order
	orderCacheMu.Unlock()

	return &order, nil
}

func saveOrderToDB(ctx context.Context, order *models.Order) (err error) {
	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback(ctx)
		}
	}()

	// order
	_, err = tx.Exec(ctx, `
        INSERT INTO orders (
            order_uid, track_number, entry, locale,
            internal_signature, customer_id, delivery_service,
            shardkey, sm_id, date_created, oof_shard
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (order_uid) DO NOTHING
    `,
		order.OrderUID, order.TrackNumber, order.Entry, order.Locale,
		order.InternalSignature, order.CustomerID, order.DeliveryService,
		order.ShardKey, order.SmID, order.DateCreated, order.OofShard,
	)
	if err != nil {
		return err
	}

	// delivery
	_, err = tx.Exec(ctx, `
        INSERT INTO deliveries (
            order_uid, name, phone, zip, city, address, region, email
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (order_uid) DO NOTHING;
    `,
		order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email,
	)
	if err != nil {
		return err
	}

	// payment
	_, err = tx.Exec(ctx, `
        INSERT INTO payments (
            order_uid, transaction, request_id, currency, provider, amount,
            payment_dt, bank, delivery_cost, goods_total, custom_fee
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (order_uid) DO NOTHING;
    `,
		order.OrderUID, order.Payment.Transaction, order.Payment.RequestID,
		order.Payment.Currency, order.Payment.Provider, order.Payment.Amount,
		order.Payment.PaymentDT, order.Payment.Bank, order.Payment.DeliveryCost,
		order.Payment.GoodsTotal, order.Payment.CustomFee,
	)
	if err != nil {
		return err
	}

	// items
	for _, item := range order.Items {
		_, err = tx.Exec(ctx, `
            INSERT INTO items (
                order_uid, chrt_id, track_number, price, rid,
                name, sale, size, total_price, nm_id, brand, status
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (order_uid, chrt_id) DO NOTHING;
        `,
			order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.RID,
			item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID,
			item.Brand, item.Status,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// ---------------------------- Kafka ----------------------------

func consumeKafka(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9094"},
		Topic:          "orders",
		GroupID:        "order-service",
		CommitInterval: 0,
	})

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Printf("Kafka consumer stopping: %v\n", err)
				return
			}
			log.Printf("Kafka fetch error: %v\n", err)
			continue
		}

		var order models.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Printf("⚠️ Invalid JSON at offset %d: %v\n", m.Offset, err)

			if commitErr := r.CommitMessages(ctx, m); commitErr != nil {
				log.Printf("⚠️ Failed to commit bad message offset %d: %v\n", m.Offset, commitErr)
			}
			continue
		}

		if err := order.Validate(); err != nil {
			log.Printf("⚠️ Invalid order (key=%s, offset=%d): %v\n", string(m.Key), m.Offset, err)

			if commitErr := r.CommitMessages(ctx, m); commitErr != nil {
				log.Printf("⚠️ Failed to commit invalid order offset %d: %v\n", m.Offset, commitErr)
			}
			continue
		}

		log.Printf("📥 New order from Kafka: %s\n", order.OrderUID)

		if err := saveOrderToDB(ctx, &order); err != nil {
			log.Printf("❌ DB insert error for offset %d (will retry): %v\n", m.Offset, err)
			continue
		}

		log.Printf("📥 Saved successfully from Kafka: %s\n", order.OrderUID)

		orderCacheMu.Lock()
		orderCache[order.OrderUID] = &order
		orderCacheMu.Unlock()

		log.Printf("✅ Cached order %s", order.OrderUID)

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("⚠️ Commit offset %d failed (message may be reprocessed): %v\n", m.Offset, err)
		} else {
			log.Printf("✅ Committed offset %d for order %s\n", m.Offset, order.OrderUID)
		}
	}
}
