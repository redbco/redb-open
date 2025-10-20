package influxdb

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements data operations for InfluxDB.
type DataOps struct {
	conn *Connection
}

// Fetch retrieves points from InfluxDB.
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
	return d.FetchWithColumns(ctx, table, nil, limit)
}

// FetchWithColumns retrieves points with specific fields.
func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return nil, fmt.Errorf("no bucket specified")
	}

	// Build Flux query
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -24h)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> limit(n: %d)
	`, bucket, table, limit)

	queryAPI := d.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}
	defer result.Close()

	rows := make([]map[string]interface{}, 0)

	for result.Next() {
		record := result.Record()
		row := make(map[string]interface{})

		row["_time"] = record.Time()
		row["_measurement"] = record.Measurement()
		row["_field"] = record.Field()
		row["_value"] = record.Value()

		// Add tags
		for k, v := range record.Values() {
			if k[0] != '_' { // Tags don't start with underscore
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}

	return rows, nil
}

// Insert writes points to InfluxDB.
func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	writeAPI := d.conn.client.GetWriteAPIBlocking()
	var count int64

	for _, row := range data {
		// Create a point
		timestamp := time.Now()
		if t, ok := row["_time"].(time.Time); ok {
			timestamp = t
		} else if ts, ok := row["timestamp"].(time.Time); ok {
			timestamp = ts
		}

		fieldName := "value"
		if f, ok := row["_field"].(string); ok {
			fieldName = f
		}

		var value interface{} = 0.0
		if v, ok := row["_value"]; ok {
			value = v
		} else if v, ok := row["value"]; ok {
			value = v
		}

		point := influxdb2.NewPoint(
			table,
			make(map[string]string),
			map[string]interface{}{fieldName: value},
			timestamp,
		)

		// Add tags
		for k, v := range row {
			if k != "_time" && k != "_field" && k != "_value" && k != "timestamp" && k != "value" {
				if strVal, ok := v.(string); ok {
					point = point.AddTag(k, strVal)
				}
			}
		}

		err := writeAPI.WritePoint(ctx, point)
		if err != nil {
			return count, fmt.Errorf("failed to write point: %w", err)
		}

		count++
	}

	return count, nil
}

// Update is not directly supported in InfluxDB (time series are append-only).
func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	return 0, fmt.Errorf("Update not supported for time-series database")
}

// Upsert writes points (InfluxDB handles duplicates by overwriting).
func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	return d.Insert(ctx, table, data)
}

// Delete removes points from InfluxDB.
func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	// Build predicate from conditions
	predicate := fmt.Sprintf(`_measurement="%s"`, table)

	// Get time range from conditions
	start := time.Now().Add(-24 * time.Hour)
	stop := time.Now()

	if startTime, ok := conditions["start"].(time.Time); ok {
		start = startTime
	}
	if stopTime, ok := conditions["stop"].(time.Time); ok {
		stop = stopTime
	}

	deleteAPI := d.conn.client.Client().DeleteAPI()
	err := deleteAPI.DeleteWithName(ctx, d.conn.client.GetOrg(), bucket, start, stop, predicate)
	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	// InfluxDB doesn't return count of deleted records
	return 1, nil
}

// Stream retrieves points in batches.
func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return adapter.StreamResult{}, fmt.Errorf("no bucket specified")
	}

	// Build Flux query with offset
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -24h)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> limit(n: %d, offset: %d)
	`, bucket, params.Table, params.BatchSize, params.Offset)

	queryAPI := d.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return adapter.StreamResult{}, fmt.Errorf("failed to stream data: %w", err)
	}
	defer result.Close()

	rows := make([]map[string]interface{}, 0)

	for result.Next() {
		record := result.Record()
		row := make(map[string]interface{})

		row["_time"] = record.Time()
		row["_measurement"] = record.Measurement()
		row["_field"] = record.Field()
		row["_value"] = record.Value()

		for k, v := range record.Values() {
			if k[0] != '_' {
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	if result.Err() != nil {
		return adapter.StreamResult{}, fmt.Errorf("query error: %w", result.Err())
	}

	hasMore := len(rows) == int(params.BatchSize)

	return adapter.StreamResult{
		Data:       rows,
		HasMore:    hasMore,
		NextCursor: fmt.Sprintf("%d", params.Offset+int64(len(rows))),
	}, nil
}

// ExecuteQuery executes a Flux query.
func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
	queryAPI := d.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer result.Close()

	var results []interface{}

	for result.Next() {
		record := result.Record()
		results = append(results, record.Values())
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}

	return results, nil
}

// ExecuteCountQuery counts points.
func (d *DataOps) ExecuteCountQuery(ctx context.Context, query string) (int64, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, fmt.Errorf("no bucket specified")
	}

	countQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -24h)
			|> count()
	`, bucket)

	queryAPI := d.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, countQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to count: %w", err)
	}
	defer result.Close()

	var count int64
	for result.Next() {
		if val, ok := result.Record().Value().(int64); ok {
			count += val
		}
	}

	if result.Err() != nil {
		return 0, fmt.Errorf("query error: %w", result.Err())
	}

	return count, nil
}

// GetRowCount returns the number of points.
func (d *DataOps) GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error) {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return 0, false, fmt.Errorf("no bucket specified")
	}

	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -24h)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> count()
	`, bucket, table)

	queryAPI := d.conn.client.GetQueryAPI()
	result, err := queryAPI.Query(ctx, query)
	if err != nil {
		return 0, false, fmt.Errorf("failed to count: %w", err)
	}
	defer result.Close()

	var count int64
	for result.Next() {
		if val, ok := result.Record().Value().(int64); ok {
			count += val
		}
	}

	if result.Err() != nil {
		return 0, false, fmt.Errorf("query error: %w", result.Err())
	}

	return count, true, nil
}

// Wipe deletes all data in the bucket.
func (d *DataOps) Wipe(ctx context.Context) error {
	bucket := d.conn.client.GetBucket()
	if bucket == "" {
		return fmt.Errorf("no bucket specified")
	}

	deleteAPI := d.conn.client.Client().DeleteAPI()
	start := time.Unix(0, 0)
	stop := time.Now()

	err := deleteAPI.DeleteWithName(ctx, d.conn.client.GetOrg(), bucket, start, stop, "")
	if err != nil {
		return fmt.Errorf("failed to wipe data: %w", err)
	}

	return nil
}
