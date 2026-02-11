package kafkarator

import "fmt"

type autoOffsetReset string

const (
	offsetEarliest autoOffsetReset = "earliest"
	offsetLatest   autoOffsetReset = "latest"
)

func (v autoOffsetReset) validate() {
	switch v {
	case offsetEarliest, offsetLatest:
		return
	default:
		panic(fmt.Sprintf("invalid AutoOffsetReset: %q", v))
	}
}
