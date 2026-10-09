package budgetprocessor // import "github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/paulojmdias/otel-budget-processor/processor/budgetprocessor/internal/metadata"
)

var capabilities = processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true})

// NewFactory returns the budget processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogs, metadata.LogsStability),
		processor.WithTraces(createTraces, metadata.TracesStability),
		processor.WithMetrics(createMetrics, metadata.MetricsStability),
	)
}

func createLogs(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	p := newProcessor(cfg.(*Config), set.Logger)
	return processorhelper.NewLogs(ctx, set, cfg, next, p.processLogs, capabilities, processorhelper.WithStart(p.start))
}

func createTraces(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	p := newProcessor(cfg.(*Config), set.Logger)
	return processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces, capabilities, processorhelper.WithStart(p.start))
}

func createMetrics(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	p := newProcessor(cfg.(*Config), set.Logger)
	return processorhelper.NewMetrics(ctx, set, cfg, next, p.processMetrics, capabilities, processorhelper.WithStart(p.start))
}
