package kafkarator

import (
	"math"
	"testing"
)

func Test_newReadCloser(t *testing.T) {
}

func Test_nan(t *testing.T) {
	var x float64 = math.NaN()

	_ = x
}
