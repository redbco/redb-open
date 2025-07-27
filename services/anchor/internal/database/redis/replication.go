package redis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"github.com/redis/go-redis/v9"
)

// CreateReplicationSource sets up a replication source using Redis keyspace notifications
func CreateReplicationSource(client *redis.Client, keyPattern string, databaseID string, eventHandler func(map[string]interface{})) (*RedisReplicationSourceDetails, error) {
	ctx := context.Background()

	// Generate a unique channel name for this replication
	channelName := fmt.Sprintf("keyevent_%s_%s", databaseID, common.GenerateUniqueID())

	// Enable keyspace notifications if not already enabled
	result, err := client.Do(ctx, "CONFIG", "GET", "notify-keyspace-events").Result()
	if err != nil {
		return nil, fmt.Errorf("error getting Redis config: %v", err)
	}

	currentConfig := ""
	if resultSlice, ok := result.([]interface{}); ok && len(resultSlice) > 1 {
		if configVal, ok := resultSlice[1].(string); ok {
			currentConfig = configVal
		}
	}

	// We need at least "KEA" for keyspace events (Keys, Events, string commands)
	requiredConfig := "KEA"
	newConfig := currentConfig

	// Check if all required flags are present
	for _, c := range requiredConfig {
		if !strings.ContainsRune(currentConfig, c) {
			newConfig += string(c)
		}
	}

	// Update config if needed
	if newConfig != currentConfig {
		_, err = client.Do(ctx, "CONFIG", "SET", "notify-keyspace-events", newConfig).Result()
		if err != nil {
			return nil, fmt.Errorf("error setting Redis config: %v", err)
		}
	}

	details := &RedisReplicationSourceDetails{
		KeyPattern:  keyPattern,
		ChannelName: channelName,
		DatabaseID:  databaseID,
	}

	// Start listening for keyspace events
	go listenForKeyspaceEvents(client, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(client *redis.Client, details *RedisReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Start listening for keyspace events
	go listenForKeyspaceEvents(client, details, eventHandler)

	return nil
}

func listenForKeyspaceEvents(client *redis.Client, details *RedisReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	ctx := context.Background()

	// Create a new pubsub client
	pubsub := client.PSubscribe(ctx, "__keyspace@*__:*")
	defer pubsub.Close()

	// Channel to receive messages
	ch := pubsub.Channel()

	// Keep track of previous values for update operations
	previousValues := make(map[string]interface{})

	// Use for range instead of for { select {} }
	for msg := range ch {
		// Extract key from channel pattern
		// Format: __keyspace@0__:mykey
		parts := strings.Split(msg.Channel, ":")
		if len(parts) < 2 {
			continue
		}

		key := strings.Join(parts[1:], ":")
		operation := msg.Payload

		// Check if key matches our pattern
		if !matchesPattern(key, details.KeyPattern) {
			continue
		}

		// Get the current value
		var currentValue interface{}

		if operation != "del" && operation != "expired" {
			keyType, err := client.Type(ctx, key).Result()
			if err != nil {
				continue
			}

			// Get value based on type
			switch keyType {
			case "string":
				currentValue, err = client.Get(ctx, key).Result()
			case "list":
				currentValue, err = client.LRange(ctx, key, 0, -1).Result()
			case "set":
				currentValue, err = client.SMembers(ctx, key).Result()
			case "zset":
				currentValue, err = client.ZRangeWithScores(ctx, key, 0, -1).Result()
			case "hash":
				currentValue, err = client.HGetAll(ctx, key).Result()
			case "stream":
				// For streams, get the latest entries
				currentValue, err = client.XRange(ctx, key, "-", "+").Result()
			}

			if err != nil {
				continue
			}
		}

		// Determine the operation type
		var eventType string
		var oldValue interface{}

		switch operation {
		case "set":
			if _, exists := previousValues[key]; exists {
				eventType = "update"
				oldValue = previousValues[key]
			} else {
				eventType = "insert"
			}
			previousValues[key] = currentValue
		case "del", "expired":
			eventType = "delete"
			oldValue = previousValues[key]
			delete(previousValues, key)
		default:
			// For other operations like lpush, hset, etc.
			if _, exists := previousValues[key]; exists {
				eventType = "update"
				oldValue = previousValues[key]
			} else {
				eventType = "insert"
			}
			previousValues[key] = currentValue
		}

		// Create event data
		event := map[string]interface{}{
			"key":       key,
			"operation": eventType,
			"data":      currentValue,
			"old_data":  oldValue,
			"command":   operation,
		}

		// Send event to handler
		eventHandler(event)
	}

	// If we exit the loop, try to reconnect
	log.Println("Redis pubsub channel closed, reconnecting...")
	time.Sleep(5 * time.Second)
	go listenForKeyspaceEvents(client, details, eventHandler)
}

// matchesPattern checks if a key matches a pattern (simplified version)
func matchesPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}

	// Simple pattern matching for now
	// In a real implementation, you might want to use a more sophisticated pattern matching
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(key, prefix)
	}

	return key == pattern
}
