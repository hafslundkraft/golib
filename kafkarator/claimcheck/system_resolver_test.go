package claimcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeriveSystemFromTopic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		topic string
		want  string
	}{
		{"internal product", "test.sys--billing.invoices--v1", "billing"},
		{"shared product", "test.water--obs.measurements--v1", "data-definitions"},
		{"internal in prod env", "prod.sys--metering.readings--v2", "metering"},
		{"unqualified name with dots", "test.sys--billing.invoices.extra", "billing"},
		{"two segments only", "test.invoices", ""},
		{"single segment", "invoices", ""},
		{"no double-dash in domain", "test.billing.invoices", ""},
		{"empty system after sys--", "test.sys--.invoices", ""},
		{"empty topic", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, deriveSystemFromTopic(tc.topic))
		})
	}
}
