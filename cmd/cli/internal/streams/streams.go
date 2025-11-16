package streams

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type Stream struct {
	TenantID          string                 `json:"tenant_id"`
	WorkspaceID       string                 `json:"workspace_id"`
	StreamID          string                 `json:"stream_id"`
	StreamName        string                 `json:"stream_name"`
	StreamDescription string                 `json:"stream_description"`
	StreamPlatform    string                 `json:"stream_platform"`
	StreamVersion     string                 `json:"stream_version"`
	RegionID          *string                `json:"region_id"`
	ConnectionConfig  map[string]interface{} `json:"connection_config"`
	CredentialKey     string                 `json:"credential_key"`
	Metadata          map[string]interface{} `json:"metadata"`
	MonitoredTopics   []string               `json:"monitored_topics"`
	ConnectedToNodeID int64                  `json:"connected_to_node_id"`
	OwnerID           string                 `json:"owner_id"`
	Status            string                 `json:"status"`
	Created           string                 `json:"created"`
	Updated           string                 `json:"updated"`
}

type TopicInfo struct {
	Name       string            `json:"name"`
	Partitions int32             `json:"partitions"`
	Replicas   int32             `json:"replicas"`
	Config     map[string]string `json:"config"`
}

type TopicSchema struct {
	TopicName       string          `json:"topic_name"`
	Schema          json.RawMessage `json:"schema"`
	MessagesSampled int64           `json:"messages_sampled"`
	ConfidenceScore float64         `json:"confidence_score"`
}

// ListStreams lists all stream connections using profile-based authentication
func ListStreams() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/streams")
	if err != nil {
		return err
	}

	var streamsResponse struct {
		Streams []Stream `json:"streams"`
	}
	if err := client.Get(url, &streamsResponse); err != nil {
		return fmt.Errorf("failed to list streams: %v", err)
	}

	if len(streamsResponse.Streams) == 0 {
		fmt.Println("No stream connections found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Println()
	fmt.Fprintln(w, "Name\tPlatform\tStatus\tTopics\tNode ID")
	fmt.Fprintln(w, "----\t--------\t------\t------\t-------")
	for _, stream := range streamsResponse.Streams {
		topicsCount := len(stream.MonitoredTopics)
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\n",
			stream.StreamName,
			stream.StreamPlatform,
			stream.Status,
			topicsCount,
			stream.ConnectedToNodeID)
	}
	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowStream displays details of a specific stream connection
func ShowStream(streamName string, args []string) error {
	streamName = strings.TrimSpace(streamName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s", streamName))
	if err != nil {
		return err
	}

	var streamResponse struct {
		Stream Stream `json:"stream"`
	}
	if err := client.Get(url, &streamResponse); err != nil {
		return fmt.Errorf("failed to get stream details: %v", err)
	}

	stream := streamResponse.Stream
	fmt.Println()
	fmt.Printf("Stream Connection Details for '%s'\n", stream.StreamName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("ID:                    %s\n", stream.StreamID)
	fmt.Printf("Name:                  %s\n", stream.StreamName)
	fmt.Printf("Description:           %s\n", stream.StreamDescription)
	fmt.Printf("Platform:              %s\n", stream.StreamPlatform)
	fmt.Printf("Version:               %s\n", stream.StreamVersion)
	fmt.Printf("Status:                %s\n", stream.Status)
	fmt.Printf("Connected to Node ID:  %d\n", stream.ConnectedToNodeID)
	fmt.Printf("Owner ID:              %s\n", stream.OwnerID)
	fmt.Printf("Tenant ID:             %s\n", stream.TenantID)
	fmt.Printf("Workspace ID:          %s\n", stream.WorkspaceID)
	if stream.RegionID != nil {
		fmt.Printf("Region ID:             %s\n", *stream.RegionID)
	}
	fmt.Printf("Created:               %s\n", stream.Created)
	fmt.Printf("Updated:               %s\n", stream.Updated)

	// Display connection config
	if len(stream.ConnectionConfig) > 0 {
		fmt.Println()
		fmt.Println("Connection Configuration:")
		fmt.Println(strings.Repeat("-", 50))
		for key, value := range stream.ConnectionConfig {
			// Mask sensitive fields
			if strings.Contains(strings.ToLower(key), "password") ||
				strings.Contains(strings.ToLower(key), "secret") ||
				strings.Contains(strings.ToLower(key), "key") {
				fmt.Printf("  %s: ********\n", key)
			} else {
				fmt.Printf("  %s: %v\n", key, value)
			}
		}
	}

	// Display monitored topics
	if len(stream.MonitoredTopics) > 0 {
		fmt.Println()
		fmt.Println("Monitored Topics:")
		fmt.Println(strings.Repeat("-", 50))
		for _, topic := range stream.MonitoredTopics {
			fmt.Printf("  - %s\n", topic)
		}
	}

	fmt.Println()

	// Check for topics flag
	showTopics := false
	for _, arg := range args {
		if arg == "--topics" {
			showTopics = true
		}
	}

	// Display topics if requested
	if showTopics {
		fmt.Println("Fetching topics...")
		if err := ListTopics(streamName); err != nil {
			return fmt.Errorf("failed to list topics: %v", err)
		}
	}

	return nil
}

// ConnectStream connects to a new streaming platform
func ConnectStream(args []string) error {
	reader := bufio.NewReader(os.Stdin)
	argsMap := scanArgs(args)

	// Get stream name
	streamName := getArgOrPrompt(reader, argsMap, "name", "Stream Name: ", true)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	// Get stream description
	streamDescription := getArgOrPrompt(reader, argsMap, "description", "Description (optional): ", false)

	// Get platform
	platform := getArgOrPrompt(reader, argsMap, "platform", "Platform (kafka, kinesis, pubsub, eventhubs, pulsar, rabbitmq, nats, mqtt, sqs, sns): ", true)
	if platform == "" {
		return fmt.Errorf("platform is required")
	}

	// Build connection config based on platform
	connectionConfig := make(map[string]interface{})

	switch strings.ToLower(platform) {
	case "kafka", "redpanda":
		brokers := getArgOrPrompt(reader, argsMap, "brokers", "Brokers (comma-separated): ", true)
		if brokers == "" {
			return fmt.Errorf("brokers are required for Kafka/Redpanda")
		}
		connectionConfig["brokers"] = strings.Split(brokers, ",")

		username := getArgOrPrompt(reader, argsMap, "username", "Username (optional): ", false)
		if username != "" {
			connectionConfig["username"] = username
			password := getArgOrPrompt(reader, argsMap, "password", "Password: ", false)
			connectionConfig["password"] = password
		}

		tlsEnabled := getArgOrPrompt(reader, argsMap, "tls", "Enable TLS? (yes/no) [no]: ", false)
		if strings.ToLower(tlsEnabled) == "yes" || strings.ToLower(tlsEnabled) == "y" {
			connectionConfig["tls_enabled"] = true
		}

		groupID := getArgOrPrompt(reader, argsMap, "group-id", "Consumer Group ID (optional): ", false)
		if groupID != "" {
			connectionConfig["group_id"] = groupID
		}

	case "kinesis":
		region := getArgOrPrompt(reader, argsMap, "region", "AWS Region: ", true)
		if region == "" {
			return fmt.Errorf("region is required for Kinesis")
		}
		connectionConfig["region"] = region

		accessKey := getArgOrPrompt(reader, argsMap, "access-key", "Access Key (optional): ", false)
		if accessKey != "" {
			connectionConfig["access_key"] = accessKey
			secretKey := getArgOrPrompt(reader, argsMap, "secret-key", "Secret Key: ", false)
			connectionConfig["secret_key"] = secretKey
		}

	case "pubsub":
		projectID := getArgOrPrompt(reader, argsMap, "project-id", "GCP Project ID: ", true)
		if projectID == "" {
			return fmt.Errorf("project ID is required for Pub/Sub")
		}
		connectionConfig["project_id"] = projectID

		credentialsPath := getArgOrPrompt(reader, argsMap, "credentials", "Credentials JSON Path (optional): ", false)
		if credentialsPath != "" {
			connectionConfig["credentials_path"] = credentialsPath
		}

	case "eventhubs":
		namespace := getArgOrPrompt(reader, argsMap, "namespace", "Event Hubs Namespace: ", true)
		if namespace == "" {
			return fmt.Errorf("namespace is required for Event Hubs")
		}
		connectionConfig["namespace"] = namespace

		connectionString := getArgOrPrompt(reader, argsMap, "connection-string", "Connection String: ", true)
		if connectionString == "" {
			return fmt.Errorf("connection string is required for Event Hubs")
		}
		connectionConfig["connection_string"] = connectionString

	default:
		fmt.Printf("Platform '%s' requires manual configuration.\n", platform)
		fmt.Println("Please provide connection configuration as key=value pairs (empty line to finish):")
		for {
			fmt.Print("  ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			if input == "" {
				break
			}
			parts := strings.SplitN(input, "=", 2)
			if len(parts) == 2 {
				connectionConfig[parts[0]] = parts[1]
			}
		}
	}

	// Get monitored topics
	topics := getArgOrPrompt(reader, argsMap, "topics", "Topics to monitor (comma-separated, optional): ", false)
	var monitoredTopics []string
	if topics != "" {
		monitoredTopics = strings.Split(topics, ",")
		for i, topic := range monitoredTopics {
			monitoredTopics[i] = strings.TrimSpace(topic)
		}
	}

	// Get node ID
	nodeIDStr := getArgOrPrompt(reader, argsMap, "node-id", "Node ID (optional): ", false)
	var nodeID int64
	if nodeIDStr != "" {
		var err error
		nodeID, err = strconv.ParseInt(nodeIDStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid node ID: %v", err)
		}
	}

	// Create the connect request
	connectReq := map[string]interface{}{
		"stream_name":        streamName,
		"stream_description": streamDescription,
		"stream_platform":    platform,
		"connection_config":  connectionConfig,
		"monitored_topics":   monitoredTopics,
		"node_id":            nodeID,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/streams/connect")
	if err != nil {
		return err
	}

	var connectResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Stream  Stream `json:"stream"`
	}
	if err := client.Post(url, connectReq, &connectResponse); err != nil {
		return fmt.Errorf("failed to connect stream: %v", err)
	}

	fmt.Printf("Successfully connected stream '%s' (ID: %s)\n", connectResponse.Stream.StreamName, connectResponse.Stream.StreamID)
	return nil
}

// ModifyStream modifies an existing stream connection
func ModifyStream(streamName string, args []string) error {
	streamName = strings.TrimSpace(streamName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	// First get the stream to show current values
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s", streamName))
	if err != nil {
		return err
	}

	fmt.Println()

	var response struct {
		Stream Stream `json:"stream"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get stream: %v", err)
	}

	targetStream := response.Stream
	reader := bufio.NewReader(os.Stdin)
	updateReq := make(map[string]interface{})
	hasChanges := false

	// Parse command line arguments or prompt for input
	argsMap := scanArgs(args)

	// If no arguments provided, prompt for input
	if len(args) == 0 {
		fmt.Printf("Modifying stream '%s' (press Enter to keep current value):\n", streamName)

		fmt.Printf("Description [%s]: ", targetStream.StreamDescription)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq["stream_description"] = newDescription
			hasChanges = true
		}

		fmt.Println("Monitored Topics (comma-separated) [current: " + strings.Join(targetStream.MonitoredTopics, ", ") + "]: ")
		newTopics, _ := reader.ReadString('\n')
		newTopics = strings.TrimSpace(newTopics)
		if newTopics != "" {
			topics := strings.Split(newTopics, ",")
			for i, topic := range topics {
				topics[i] = strings.TrimSpace(topic)
			}
			updateReq["monitored_topics"] = topics
			hasChanges = true
		}
	} else {
		// Parse flags
		if desc, ok := argsMap["description"]; ok {
			updateReq["stream_description"] = desc
			hasChanges = true
		}
		if topics, ok := argsMap["topics"]; ok {
			topicList := strings.Split(topics, ",")
			for i, topic := range topicList {
				topicList[i] = strings.TrimSpace(topic)
			}
			updateReq["monitored_topics"] = topicList
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the stream
	updateURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s", streamName))
	if err != nil {
		return err
	}

	var updateResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Stream  Stream `json:"stream"`
	}
	if err := client.Put(updateURL, updateReq, &updateResponse); err != nil {
		return fmt.Errorf("failed to update stream: %v", err)
	}

	fmt.Printf("Successfully updated stream '%s'\n", updateResponse.Stream.StreamName)
	fmt.Println()
	return nil
}

// ReconnectStream reconnects an existing stream connection
func ReconnectStream(streamName string) error {
	streamName = strings.TrimSpace(streamName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s/reconnect", streamName))
	if err != nil {
		return err
	}

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Stream  Stream `json:"stream"`
	}
	if err := client.Post(url, nil, &response); err != nil {
		return fmt.Errorf("failed to reconnect stream: %v", err)
	}

	fmt.Printf("Successfully reconnected stream '%s'\n", streamName)
	return nil
}

// DisconnectStream disconnects a stream connection
func DisconnectStream(streamName string, args []string) error {
	streamName = strings.TrimSpace(streamName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	// Check for delete flag
	deleteStream := false
	for _, arg := range args {
		if arg == "--delete" {
			deleteStream = true
		}
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Confirm disconnection
	reader := bufio.NewReader(os.Stdin)
	fmt.Println()
	fmt.Printf("Are you sure you want to disconnect stream '%s'? (y/N): ", streamName)
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(strings.ToLower(confirmation))

	if confirmation != "y" && confirmation != "yes" {
		fmt.Println("Operation cancelled")
		fmt.Println()
		return nil
	}

	if !deleteStream {
		fmt.Print("Delete stream metadata as well? (y/N): ")
		deleteConfirmation, _ := reader.ReadString('\n')
		deleteConfirmation = strings.TrimSpace(strings.ToLower(deleteConfirmation))
		if deleteConfirmation == "y" || deleteConfirmation == "yes" {
			deleteStream = true
		}
	}

	// Create disconnect request
	disconnectReq := map[string]interface{}{
		"delete_stream": deleteStream,
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s/disconnect", streamName))
	if err != nil {
		return err
	}

	var disconnectResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
	}
	if err := client.Post(url, disconnectReq, &disconnectResponse); err != nil {
		return fmt.Errorf("failed to disconnect stream: %v", err)
	}

	fmt.Printf("Successfully disconnected stream '%s'\n", streamName)
	if deleteStream {
		fmt.Println("Stream metadata has been deleted")
	}
	fmt.Println()
	return nil
}

// ListTopics lists topics for a stream connection
func ListTopics(streamName string) error {
	streamName = strings.TrimSpace(streamName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s/topics", streamName))
	if err != nil {
		return err
	}

	var topicsResponse struct {
		Success bool        `json:"success"`
		Topics  []TopicInfo `json:"topics"`
	}
	if err := client.Get(url, &topicsResponse); err != nil {
		return fmt.Errorf("failed to list topics: %v", err)
	}

	if len(topicsResponse.Topics) == 0 {
		fmt.Println("No topics found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Println()
	fmt.Println("Topics:")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Fprintln(w, "Name\tPartitions\tReplicas")
	fmt.Fprintln(w, "----\t----------\t--------")
	for _, topic := range topicsResponse.Topics {
		fmt.Fprintf(w, "%s\t%d\t%d\n",
			topic.Name,
			topic.Partitions,
			topic.Replicas)
	}
	_ = w.Flush()
	fmt.Println()
	return nil
}

// GetTopicSchema gets the discovered schema for a topic
func GetTopicSchema(streamName, topicName string) error {
	streamName = strings.TrimSpace(streamName)
	topicName = strings.TrimSpace(topicName)
	if streamName == "" {
		return fmt.Errorf("stream name is required")
	}
	if topicName == "" {
		return fmt.Errorf("topic name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/streams/%s/topics/%s/schema", streamName, topicName))
	if err != nil {
		return err
	}

	var schemaResponse struct {
		Success         bool            `json:"success"`
		TopicName       string          `json:"topic_name"`
		Schema          json.RawMessage `json:"schema"`
		MessagesSampled int64           `json:"messages_sampled"`
		ConfidenceScore float64         `json:"confidence_score"`
	}
	if err := client.Get(url, &schemaResponse); err != nil {
		return fmt.Errorf("failed to get topic schema: %v", err)
	}

	fmt.Println()
	fmt.Printf("Schema for Topic '%s'\n", schemaResponse.TopicName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Messages Sampled:  %d\n", schemaResponse.MessagesSampled)
	fmt.Printf("Confidence Score:  %.2f\n", schemaResponse.ConfidenceScore)
	fmt.Println()

	// Pretty print the schema
	if len(schemaResponse.Schema) > 0 {
		var prettySchema interface{}
		if err := json.Unmarshal(schemaResponse.Schema, &prettySchema); err == nil {
			prettyJSON, err := json.MarshalIndent(prettySchema, "", "  ")
			if err == nil {
				fmt.Println("Schema:")
				fmt.Println(string(prettyJSON))
			}
		}
	} else {
		fmt.Println("No schema available yet. Schema discovery may still be in progress.")
	}

	fmt.Println()
	return nil
}

// Helper functions

func scanArgs(args []string) map[string]string {
	argsMap := make(map[string]string)
	for _, arg := range args {
		if strings.HasPrefix(arg, "--") {
			parts := strings.SplitN(arg[2:], "=", 2)
			if len(parts) == 2 {
				argsMap[parts[0]] = parts[1]
			} else if len(parts) == 1 {
				argsMap[parts[0]] = ""
			}
		}
	}
	return argsMap
}

func getArgOrPrompt(reader *bufio.Reader, argsMap map[string]string, key, prompt string, trim bool) string {
	if val, ok := argsMap[key]; ok {
		return val
	}

	if prompt != "" {
		fmt.Print(prompt)
		value, _ := reader.ReadString('\n')
		if trim {
			return strings.TrimSpace(value)
		}
		return value
	}

	return ""
}
