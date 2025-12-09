package telemetry

import (
	"context"
	"fmt"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func Test_deterministicIDGenerator_NewSpanID(t *testing.T) {
	type newSpanIDTestCase struct {
		seed       int64
		iterations int
	}
	tests := []newSpanIDTestCase{
		{
			seed:       defaultSeed,
			iterations: 150,
		},
		{
			seed:       90099,
			iterations: 150,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.seed), func(t *testing.T) {
			snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory("testdata"))
			sb := strings.Builder{}
			gen := newDeterministicIDGenerator(tt.seed)
			traceID, spanID := gen.NewIDs(ctx)
			sb.WriteString(fmt.Sprintf("%s\n", traceID.String()))
			sb.WriteString(fmt.Sprintf("%s\n", spanID.String()))
			for i := 0; i < tt.iterations; i++ {
				spanID = gen.NewSpanID(ctx, traceID)
				sb.WriteString(fmt.Sprintf("%s\n", spanID.String()))
			}
			snapshotter.SnapshotT(t, sb.String())
		})
	}
}

func Test_deterministicIDGenerator_NewIDs(t *testing.T) {
	type newIDsTestCase struct {
		seed            int64
		expectedTraceID string
		expectedSpanID  string
	}

	testCases := []newIDsTestCase{
		{
			seed:            42,
			expectedTraceID: "afbf64b1967f8c538872b44b9fbb971b",
			expectedSpanID:  "4d52f284145b9fe8",
		},
		{
			seed:            213214323,
			expectedTraceID: "bb1ff844bf538a4b70a5fd20a5f62afb",
			expectedSpanID:  "8f632259cea5b940",
		},
	}

	ctx := context.Background()

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%d", testCase.seed), func(t *testing.T) {
			gen := newDeterministicIDGenerator(testCase.seed)
			traceID, spanID := gen.NewIDs(ctx)
			require.Equal(t, testCase.expectedTraceID, traceID.String())
			require.Equal(t, testCase.expectedSpanID, spanID.String())
		})
	}
}
