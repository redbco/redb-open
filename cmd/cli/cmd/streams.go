package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/streams"
	"github.com/spf13/cobra"
)

// streamsCmd represents the streams command
var streamsCmd = &cobra.Command{
	Use:   "streams",
	Short: "Manage stream connections",
	Long: "Commands for managing stream connections including listing, showing details, connecting, modifying, " +
		"reconnecting, disconnecting, and inspecting topics and schemas.",
}

// listStreamsCmd represents the list command
var listStreamsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stream connections",
	Long:  `Display a formatted list of all stream connections with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.ListStreams()
	},
}

// showStreamCmd represents the show command
var showStreamCmd = &cobra.Command{
	Use:   "show [stream-name]",
	Short: "Show stream connection details",
	Long:  `Display detailed information about a specific stream connection.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Build args slice to pass to ShowStream function
		var flags []string

		// Check if --topics flag is set
		if cmd.Flags().Lookup("topics").Changed {
			flags = append(flags, "--topics")
		}

		return streams.ShowStream(args[0], flags)
	},
}

// connectStreamCmd represents the connect command
var connectStreamCmd = &cobra.Command{
	Use:   "connect",
	Short: "Connect to a new stream",
	Long: `Connect to a new streaming platform by providing connection details interactively or using flags.

Examples:
  # Interactive mode
  redb streams connect

  # Using flags for Kafka
  redb streams connect --name my-kafka --platform kafka --brokers kafka1:9092,kafka2:9092 --topics orders,payments

  # Using flags for AWS Kinesis
  redb streams connect --name my-kinesis --platform kinesis --region us-east-1 --stream-name my-stream`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.ConnectStream(args)
	},
}

// modifyStreamCmd represents the modify command
var modifyStreamCmd = &cobra.Command{
	Use:   "modify [stream-name]",
	Short: "Modify an existing stream connection",
	Long:  `Modify an existing stream connection by providing the stream name and new details interactively or using flags.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.ModifyStream(args[0], args[1:])
	},
}

// reconnectStreamCmd represents the reconnect command
var reconnectStreamCmd = &cobra.Command{
	Use:   "reconnect [stream-name]",
	Short: "Reconnect a stream connection",
	Long:  `Reconnect a stream connection by providing the stream name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.ReconnectStream(args[0])
	},
}

// disconnectStreamCmd represents the disconnect command
var disconnectStreamCmd = &cobra.Command{
	Use:   "disconnect [stream-name]",
	Short: "Disconnect a stream connection",
	Long:  `Disconnect a stream connection by providing the stream name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.DisconnectStream(args[0], args[1:])
	},
}

// listTopicsCmd represents the topics list command
var listTopicsCmd = &cobra.Command{
	Use:   "topics [stream-name]",
	Short: "List topics for a stream",
	Long:  `List all topics/queues available in the stream connection.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.ListTopics(args[0])
	},
}

// getTopicSchemaCmd represents the schema command
var getTopicSchemaCmd = &cobra.Command{
	Use:   "schema [stream-name] [topic-name]",
	Short: "Get schema for a topic",
	Long:  `Display the discovered schema for a specific topic in the stream.`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streams.GetTopicSchema(args[0], args[1])
	},
}

func init() {
	// Add flags to showStreamCmd
	showStreamCmd.Flags().Bool("topics", false, "Show topics information")

	// Add flags to connectStreamCmd
	connectStreamCmd.Flags().String("name", "", "Stream connection name")
	connectStreamCmd.Flags().String("description", "", "Stream description")
	connectStreamCmd.Flags().String("platform", "", "Stream platform (kafka, kinesis, pubsub, eventhubs, etc.)")
	connectStreamCmd.Flags().String("brokers", "", "Comma-separated list of broker addresses (for Kafka/Redpanda)")
	connectStreamCmd.Flags().String("region", "", "AWS region (for Kinesis/SQS/SNS)")
	connectStreamCmd.Flags().String("topics", "", "Comma-separated list of topics to monitor")
	connectStreamCmd.Flags().String("node-id", "", "Node ID")

	// Add flags to disconnectStreamCmd
	disconnectStreamCmd.Flags().Bool("delete", false, "Delete stream metadata")

	// Add subcommands to streams command
	streamsCmd.AddCommand(listStreamsCmd)
	streamsCmd.AddCommand(showStreamCmd)
	streamsCmd.AddCommand(connectStreamCmd)
	streamsCmd.AddCommand(modifyStreamCmd)
	streamsCmd.AddCommand(reconnectStreamCmd)
	streamsCmd.AddCommand(disconnectStreamCmd)
	streamsCmd.AddCommand(listTopicsCmd)
	streamsCmd.AddCommand(getTopicSchemaCmd)
}
