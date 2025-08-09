package storage

import (
	"context"
	"time"
)

// MeshStorage defines the interface for all mesh-related database operations
type MeshStorage interface {
	// Node management
	CreateNode(ctx context.Context, node *MeshNode) error
	GetNode(ctx context.Context, nodeID string) (*MeshNode, error)
	UpdateNode(ctx context.Context, node *MeshNode) error
	DeleteNode(ctx context.Context, nodeID string) error
	ListNodes(ctx context.Context, status string) ([]*MeshNode, error)
	UpdateNodeLastSeen(ctx context.Context, nodeID string, lastSeen time.Time) error

	// Link management
	CreateLink(ctx context.Context, link *MeshLink) error
	GetLink(ctx context.Context, linkID string) (*MeshLink, error)
	GetLinkBetweenNodes(ctx context.Context, aNode, bNode string) (*MeshLink, error)
	UpdateLink(ctx context.Context, link *MeshLink) error
	DeleteLink(ctx context.Context, linkID string) error
	ListNodeLinks(ctx context.Context, nodeID string) ([]*MeshLink, error)

	// LSA version management
	CreateLSAVersion(ctx context.Context, lsa *MeshLSAVersion) error
	GetLSAVersion(ctx context.Context, nodeID string, version int64) (*MeshLSAVersion, error)
	GetLatestLSAVersion(ctx context.Context, nodeID string) (*MeshLSAVersion, error)
	ListLSAVersions(ctx context.Context, nodeID string) ([]*MeshLSAVersion, error)

	// Raft consensus management
	CreateRaftGroup(ctx context.Context, group *MeshRaftGroup) error
	GetRaftGroup(ctx context.Context, groupID string) (*MeshRaftGroup, error)
	UpdateRaftGroup(ctx context.Context, group *MeshRaftGroup) error
	DeleteRaftGroup(ctx context.Context, groupID string) error
	ListRaftGroups(ctx context.Context, groupType string) ([]*MeshRaftGroup, error)

	CreateRaftLogEntry(ctx context.Context, entry *MeshRaftLog) error
	GetRaftLogEntry(ctx context.Context, groupID string, logIndex int64) (*MeshRaftLog, error)
	GetRaftLogEntries(ctx context.Context, groupID string, fromIndex, toIndex int64) ([]*MeshRaftLog, error)
	DeleteRaftLogEntries(ctx context.Context, groupID string, toIndex int64) error

	// Stream management
	CreateStream(ctx context.Context, stream *MeshStream) error
	GetStream(ctx context.Context, streamID string) (*MeshStream, error)
	UpdateStream(ctx context.Context, stream *MeshStream) error
	DeleteStream(ctx context.Context, streamID string) error
	ListStreams(ctx context.Context, tenantID string) ([]*MeshStream, error)
	ListNodeStreams(ctx context.Context, nodeID string) ([]*MeshStream, error)

	CreateStreamOffset(ctx context.Context, offset *MeshStreamOffset) error
	GetStreamOffset(ctx context.Context, streamID, nodeID string) (*MeshStreamOffset, error)
	UpdateStreamOffset(ctx context.Context, offset *MeshStreamOffset) error
	DeleteStreamOffset(ctx context.Context, streamID, nodeID string) error

	// Delivery logging
	CreateDeliveryLog(ctx context.Context, log *MeshDeliveryLog) error
	UpdateDeliveryLog(ctx context.Context, log *MeshDeliveryLog) error
	GetDeliveryLog(ctx context.Context, streamID, messageID, dstNode string) (*MeshDeliveryLog, error)
	ListDeliveryLogs(ctx context.Context, streamID string, state string) ([]*MeshDeliveryLog, error)

	// Outbox/Inbox management
	CreateOutboxMessage(ctx context.Context, msg *MeshOutbox) error
	GetOutboxMessage(ctx context.Context, streamID, messageID string) (*MeshOutbox, error)
	UpdateOutboxMessage(ctx context.Context, msg *MeshOutbox) error
	DeleteOutboxMessage(ctx context.Context, streamID, messageID string) error
	ListPendingOutboxMessages(ctx context.Context, streamID string) ([]*MeshOutbox, error)
	ListFailedOutboxMessages(ctx context.Context, streamID string) ([]*MeshOutbox, error)

	CreateInboxMessage(ctx context.Context, msg *MeshInbox) error
	GetInboxMessage(ctx context.Context, streamID, messageID string) (*MeshInbox, error)
	UpdateInboxMessage(ctx context.Context, msg *MeshInbox) error
	DeleteInboxMessage(ctx context.Context, streamID, messageID string) error
	ListInboxMessages(ctx context.Context, streamID string, processed bool) ([]*MeshInbox, error)

	// Topology management
	CreateTopologySnapshot(ctx context.Context, snapshot *MeshTopologySnapshot) error
	GetTopologySnapshot(ctx context.Context, version int64) (*MeshTopologySnapshot, error)
	GetLatestTopologySnapshot(ctx context.Context) (*MeshTopologySnapshot, error)
	ListTopologySnapshots(ctx context.Context, limit int) ([]*MeshTopologySnapshot, error)

	// Configuration management
	SetConfig(ctx context.Context, key string, value interface{}) error
	GetConfig(ctx context.Context, key string) (interface{}, error)
	DeleteConfig(ctx context.Context, key string) error
	ListConfigKeys(ctx context.Context) ([]string, error)

	// Transaction support
	WithTransaction(ctx context.Context, fn func(MeshStorage) error) error
}

// MeshNodeStorage defines the interface for node-specific operations
type MeshNodeStorage interface {
	Create(ctx context.Context, node *MeshNode) error
	Get(ctx context.Context, nodeID string) (*MeshNode, error)
	Update(ctx context.Context, node *MeshNode) error
	Delete(ctx context.Context, nodeID string) error
	List(ctx context.Context, status string) ([]*MeshNode, error)
	UpdateLastSeen(ctx context.Context, nodeID string, lastSeen time.Time) error
	Exists(ctx context.Context, nodeID string) (bool, error)
}

// MeshLinkStorage defines the interface for link-specific operations
type MeshLinkStorage interface {
	Create(ctx context.Context, link *MeshLink) error
	Get(ctx context.Context, linkID string) (*MeshLink, error)
	GetBetweenNodes(ctx context.Context, aNode, bNode string) (*MeshLink, error)
	Update(ctx context.Context, link *MeshLink) error
	Delete(ctx context.Context, linkID string) error
	ListForNode(ctx context.Context, nodeID string) ([]*MeshLink, error)
	Exists(ctx context.Context, linkID string) (bool, error)
}

// MeshStreamStorage defines the interface for stream-specific operations
type MeshStreamStorage interface {
	Create(ctx context.Context, stream *MeshStream) error
	Get(ctx context.Context, streamID string) (*MeshStream, error)
	Update(ctx context.Context, stream *MeshStream) error
	Delete(ctx context.Context, streamID string) error
	ListForTenant(ctx context.Context, tenantID string) ([]*MeshStream, error)
	ListForNode(ctx context.Context, nodeID string) ([]*MeshStream, error)
	Exists(ctx context.Context, streamID string) (bool, error)
}

// MeshRaftStorage defines the interface for Raft consensus operations
type MeshRaftStorage interface {
	CreateGroup(ctx context.Context, group *MeshRaftGroup) error
	GetGroup(ctx context.Context, groupID string) (*MeshRaftGroup, error)
	UpdateGroup(ctx context.Context, group *MeshRaftGroup) error
	DeleteGroup(ctx context.Context, groupID string) error
	ListGroups(ctx context.Context, groupType string) ([]*MeshRaftGroup, error)

	AppendLogEntry(ctx context.Context, entry *MeshRaftLog) error
	GetLogEntry(ctx context.Context, groupID string, logIndex int64) (*MeshRaftLog, error)
	GetLogEntries(ctx context.Context, groupID string, fromIndex, toIndex int64) ([]*MeshRaftLog, error)
	TruncateLog(ctx context.Context, groupID string, toIndex int64) error
}

// MeshQueueStorage defines the interface for message queuing operations
type MeshQueueStorage interface {
	// Outbox operations
	EnqueueOutbox(ctx context.Context, msg *MeshOutbox) error
	DequeueOutbox(ctx context.Context, streamID string, limit int) ([]*MeshOutbox, error)
	UpdateOutboxStatus(ctx context.Context, streamID, messageID, status string) error
	RetryFailedOutbox(ctx context.Context, streamID string, maxAttempts int) error

	// Inbox operations
	EnqueueInbox(ctx context.Context, msg *MeshInbox) error
	DequeueInbox(ctx context.Context, streamID string, limit int) ([]*MeshInbox, error)
	MarkInboxProcessed(ctx context.Context, streamID, messageID string) error
	CleanupProcessedInbox(ctx context.Context, olderThan time.Duration) error
}
