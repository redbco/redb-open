package engine

import (
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
)

// convertCoreStatus converts core v2 status to REST status
func convertStatus(grpcStatus commonv1.Status) Status {
	switch grpcStatus {
	case commonv1.Status_STATUS_HEALTHY:
		return StatusHealthy
	case commonv1.Status_STATUS_DEGRADED:
		return StatusDegraded
	case commonv1.Status_STATUS_UNHEALTHY:
		return StatusUnhealthy
	case commonv1.Status_STATUS_PENDING:
		return StatusPending
	case commonv1.Status_STATUS_UNKNOWN:
		return StatusUnknown
	case commonv1.Status_STATUS_SUCCESS:
		return StatusSuccess
	case commonv1.Status_STATUS_FAILURE:
		return StatusFailure
	case commonv1.Status_STATUS_STARTING:
		return StatusStarting
	case commonv1.Status_STATUS_STOPPING:
		return StatusStopping
	case commonv1.Status_STATUS_STOPPED:
		return StatusStopped
	case commonv1.Status_STATUS_STARTED:
		return StatusStarted
	case commonv1.Status_STATUS_CREATED:
		return StatusCreated
	case commonv1.Status_STATUS_DELETED:
		return StatusDeleted
	case commonv1.Status_STATUS_UPDATED:
		return StatusUpdated
	case commonv1.Status_STATUS_CONNECTED:
		return StatusConnected
	case commonv1.Status_STATUS_DISCONNECTED:
		return StatusDisconnected
	case commonv1.Status_STATUS_CONNECTING:
		return StatusConnecting
	case commonv1.Status_STATUS_DISCONNECTING:
		return StatusDisconnecting
	case commonv1.Status_STATUS_RECONNECTING:
		return StatusReconnecting
	case commonv1.Status_STATUS_ERROR:
		return StatusError
	case commonv1.Status_STATUS_WARNING:
		return StatusWarning
	case commonv1.Status_STATUS_INFO:
		return StatusInfo
	case commonv1.Status_STATUS_DEBUG:
		return StatusDebug
	case commonv1.Status_STATUS_TRACE:
		return StatusTrace
	case commonv1.Status_STATUS_EMPTY:
		return StatusEmpty
	case commonv1.Status_STATUS_JOINING:
		return StatusJoining
	case commonv1.Status_STATUS_LEAVING:
		return StatusLeaving
	case commonv1.Status_STATUS_SEEDING:
		return StatusSeeding
	default:
		return StatusUnknown
	}
}
