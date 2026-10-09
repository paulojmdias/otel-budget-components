package budgetprocessor // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor"

import (
	"errors"

	"go.opentelemetry.io/collector/component"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

// Config is the budget processor configuration.
type Config struct {
	// Ledger is the component ID of the budget extension that owns the
	// budgets. Default: budget.
	Ledger component.ID `mapstructure:"ledger"`
	// PriceClass is the price list usage is accounted at: "hot", or "cold"
	// for the processor in a cold (diverted) pipeline. Default: hot.
	PriceClass string `mapstructure:"price_class"`
	// Enforce applies tier actions. With false the processor only accounts
	// usage and stamps resources. Default: true.
	Enforce bool `mapstructure:"enforce"`
	// FailOpen treats a missing or unhealthy budget extension as tier 0.
	// With false, a missing extension fails startup. Default: true.
	FailOpen bool `mapstructure:"fail_open"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func createDefaultConfig() component.Config {
	return &Config{
		Ledger:     component.MustNewID("budget"),
		PriceClass: "hot",
		Enforce:    true,
		FailOpen:   true,
	}
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	var errs []error
	if c.Ledger.Type().String() == "" {
		errs = append(errs, errors.New("ledger is required"))
	}
	if _, err := budgetapi.ParsePriceClass(c.PriceClass); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
