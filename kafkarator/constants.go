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

func (v autoOffsetReset) validate() {
	switch v {
	case offsetEarliest, offsetLatest:
		return
	default:
		panic(fmt.Sprintf("invalid AutoOffsetReset: %q", v))
	}
}
