// Package testutil holds processor test helpers: a fake ledger, a host,
// and pdata fixtures.
package testutil // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor/internal/testutil"

import "go.opentelemetry.io/collector/component"

// Host is a component.Host with fixed extensions.
type Host map[component.ID]component.Component

// GetExtensions implements component.Host.
func (h Host) GetExtensions() map[component.ID]component.Component { return h }

// NopComponent is a component that is not a budget ledger.
type NopComponent struct {
	component.StartFunc
	component.ShutdownFunc
}
