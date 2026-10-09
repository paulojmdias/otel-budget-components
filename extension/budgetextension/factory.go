package budgetextension // import "github.com/paulojmdias/otel-budget-components/extension/budgetextension"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/metadata"
)

// NewFactory returns the budget extension factory.
func NewFactory() extension.Factory {
	return extension.NewFactory(metadata.Type, createDefaultConfig, create, metadata.ExtensionStability)
}

func create(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	return newExtension(cfg.(*Config), set, nil)
}
