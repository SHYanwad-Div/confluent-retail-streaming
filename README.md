# Real-time Retail Intelligence & Hesitating Shopper Rescue
Built on Confluent Cloud for AI Developer Day.

## What it does
Streams live e-commerce orders and clickstream data and turns them into real-time feeds for fulfilment, marketing and inventory teams, in seconds instead of next-day batches.

## Architecture
Datagen Source connector → `sample_data_orders` topic (JSON Schema in Schema Registry) → Flink SQL fan-out:
- `bulk_orders`: orders of 8+ units → fast-track fulfilment
- `small_orders_upsell`: orders under 2 units → upsell offers
- `item_demand_per_min`: 1-min windowed demand per product → inventory

Second pipeline: clickstream → Flink tumbling windows → hesitating-shopper alerts (3+ clicks/min). Flagged 162 shoppers in its first minute.

## Confluent features used
Connectors (Datagen) · Flink SQL stream processing · Schema Registry · Stream Lineage

## Flink SQL
```sql
CREATE TABLE bulk_orders AS
SELECT * FROM sample_data_orders WHERE orderunits >= 8;

CREATE TABLE small_orders_upsell AS
SELECT * FROM sample_data_orders WHERE orderunits < 2;

CREATE TABLE item_demand_per_min AS
SELECT window_start, window_end, itemid, COUNT(*) AS order_count
FROM TABLE(TUMBLE(TABLE sample_data_orders, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, itemid;

-- Hesitating shopper alerts
SELECT window_start, window_end, user_id, COUNT(*) AS click_count
FROM TABLE(TUMBLE(TABLE `examples`.`marketplace`.`clicks`, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, user_id
HAVING COUNT(*) >= 3;
```

## Business impact
Faster fulfilment of large orders, timely upsell nudges while customers are still engaged, and inventory decisions based on live demand. Together these improve customer experience and conversion.
