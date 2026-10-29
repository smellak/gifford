package eventstore

import "github.com/smellak/gifford/internal/protocol"

// PriorityRank maps a protocol priority to the delivery_queue rank:
// 0 URGENT, 1 HIGH, 2 NORMAL, 3 LOW. Any other value is ErrInvalidArgument.
func PriorityRank(priority protocol.Priority) (int64, error) {
	switch priority {
	case protocol.PriorityUrgent:
		return 0, nil
	case protocol.PriorityHigh:
		return 1, nil
	case protocol.PriorityNormal:
		return 2, nil
	case protocol.PriorityLow:
		return 3, nil
	default:
		return 0, ErrInvalidArgument
	}
}

// PriorityFromRank is the inverse of PriorityRank. A rank outside 0–3 is
// ErrInvalidArgument.
func PriorityFromRank(rank int64) (protocol.Priority, error) {
	switch rank {
	case 0:
		return protocol.PriorityUrgent, nil
	case 1:
		return protocol.PriorityHigh, nil
	case 2:
		return protocol.PriorityNormal, nil
	case 3:
		return protocol.PriorityLow, nil
	default:
		return "", ErrInvalidArgument
	}
}
