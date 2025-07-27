package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// FetchData retrieves data from Redis based on a pattern
func FetchData(client *redis.Client, pattern string, limit int) ([]map[string]interface{}, error) {
	if pattern == "" {
		pattern = "*"
	}

	ctx := context.Background()

	// Get keys matching the pattern
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching keys: %v", err)
	}

	// Apply limit if specified
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	var result []map[string]interface{}

	// Process each key
	for _, key := range keys {
		// Get key type
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue
		}

		// Get TTL
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		ttlSeconds := int64(-1)
		if ttl != time.Duration(-1) && ttl != time.Duration(-2) {
			ttlSeconds = int64(ttl.Seconds())
		}

		entry := map[string]interface{}{
			"key":  key,
			"type": keyType,
			"ttl":  ttlSeconds,
		}

		// Get value based on type
		switch keyType {
		case "string":
			val, err := client.Get(ctx, key).Result()
			if err == nil {
				entry["value"] = val
			}
		case "list":
			vals, err := client.LRange(ctx, key, 0, -1).Result()
			if err == nil {
				entry["value"] = vals
			}
		case "set":
			vals, err := client.SMembers(ctx, key).Result()
			if err == nil {
				entry["value"] = vals
			}
		case "zset":
			vals, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
			if err == nil {
				entry["value"] = vals
			}
		case "hash":
			vals, err := client.HGetAll(ctx, key).Result()
			if err == nil {
				entry["value"] = vals
			}
		case "stream":
			// For streams, get a limited number of entries
			vals, err := client.XRange(ctx, key, "-", "+").Result()
			if err == nil {
				entry["value"] = vals
			}
		}

		result = append(result, entry)
	}

	return result, nil
}

// InsertData inserts data into Redis
func InsertData(client *redis.Client, keyPrefix string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx := context.Background()
	var totalKeysAffected int64

	// Process each data item
	for i, item := range data {
		// Generate a key if not provided
		key, ok := item["key"].(string)
		if !ok || key == "" {
			key = fmt.Sprintf("%s:%d", keyPrefix, i)
		}

		// Get value and type
		value := item["value"]
		valueType, _ := item["type"].(string)
		if valueType == "" {
			// Determine type based on value
			switch v := value.(type) {
			case string:
				valueType = "string"
			case []interface{}:
				valueType = "list"
			case map[string]interface{}:
				valueType = "hash"
			default:
				valueType = "string"
				value = fmt.Sprintf("%v", v)
			}
		}

		// Set TTL if provided
		var ttl time.Duration = -1
		if ttlVal, ok := item["ttl"].(int64); ok && ttlVal > 0 {
			ttl = time.Duration(ttlVal) * time.Second
		}

		// Insert data based on type
		var err error
		switch strings.ToLower(valueType) {
		case "string":
			strValue, _ := value.(string)
			if ttl > 0 {
				err = client.Set(ctx, key, strValue, ttl).Err()
			} else {
				err = client.Set(ctx, key, strValue, 0).Err()
			}
		case "list":
			// Clear existing list if any
			client.Del(ctx, key)

			// Add values to list
			if listValues, ok := value.([]interface{}); ok && len(listValues) > 0 {
				stringValues := make([]interface{}, len(listValues))
				for i, v := range listValues {
					stringValues[i] = fmt.Sprintf("%v", v)
				}
				err = client.RPush(ctx, key, stringValues...).Err()
				if err == nil && ttl > 0 {
					client.Expire(ctx, key, ttl)
				}
			}
		case "set":
			// Clear existing set if any
			client.Del(ctx, key)

			// Add values to set
			if setValues, ok := value.([]interface{}); ok && len(setValues) > 0 {
				stringValues := make([]interface{}, len(setValues))
				for i, v := range setValues {
					stringValues[i] = fmt.Sprintf("%v", v)
				}
				err = client.SAdd(ctx, key, stringValues...).Err()
				if err == nil && ttl > 0 {
					client.Expire(ctx, key, ttl)
				}
			}
		case "hash":
			// Clear existing hash if any
			client.Del(ctx, key)

			// Add values to hash
			if hashValues, ok := value.(map[string]interface{}); ok && len(hashValues) > 0 {
				stringHashValues := make(map[string]interface{})
				for k, v := range hashValues {
					stringHashValues[k] = fmt.Sprintf("%v", v)
				}
				err = client.HSet(ctx, key, stringHashValues).Err()
				if err == nil && ttl > 0 {
					client.Expire(ctx, key, ttl)
				}
			}
		case "zset":
			// Clear existing sorted set if any
			client.Del(ctx, key)

			// Add values to sorted set
			if zsetValues, ok := value.([]interface{}); ok && len(zsetValues) > 0 {
				for i := 0; i < len(zsetValues); i += 2 {
					if i+1 < len(zsetValues) {
						score, ok := zsetValues[i].(float64)
						if !ok {
							// Try to convert to float64
							score = 0
						}
						member := fmt.Sprintf("%v", zsetValues[i+1])
						client.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
					}
				}
				if ttl > 0 {
					client.Expire(ctx, key, ttl)
				}
			}
		default:
			err = fmt.Errorf("unsupported Redis data type: %s", valueType)
		}

		if err != nil {
			return totalKeysAffected, fmt.Errorf("error inserting data for key %s: %v", key, err)
		}

		totalKeysAffected++
	}

	return totalKeysAffected, nil
}

// WipeDatabase removes all data from the Redis database
func WipeDatabase(client *redis.Client) error {
	ctx := context.Background()

	// Flush the current database
	err := client.Do(ctx, "FLUSHDB").Err()
	if err != nil {
		return fmt.Errorf("error flushing database: %v", err)
	}

	return nil
}
