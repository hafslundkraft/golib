package kafkarator

import "fmt"

// AutoOffsetReset represents the policy for where to start consuming when no committed offset exists
type AutoOffsetReset string

const (
	// OffsetEarliest starts consuming from the earliest available offset
	OffsetEarliest AutoOffsetReset = "earliest"

	// OffsetLatest starts consuming from the latest offset
	OffsetLatest AutoOffsetReset = "latest"
)

func (v AutoOffsetReset) validate() error {
	switch v {
	case OffsetEarliest, OffsetLatest:
		return nil
	default:
		return fmt.Errorf("invalid AutoOffsetReset: %q", v)
	}
}
