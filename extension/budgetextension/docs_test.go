package budgetextension

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"gopkg.in/yaml.v3"
)

// README.md lists the defaults; keep them equal to createDefaultConfig.
func TestDocsDefaultsMatchCode(t *testing.T) {
	md, err := os.ReadFile("README.md")
	require.NoError(t, err)
	s := string(md)
	start := strings.Index(s, "<!-- defaults:start -->")
	end := strings.Index(s, "<!-- defaults:end -->")
	require.Positive(t, start)
	block := s[start:end]
	block = block[strings.Index(block, "```yaml")+len("```yaml") : strings.LastIndex(block, "```")]
	var raw map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(block), &raw))

	fromDocs := &Config{}
	require.NoError(t, confmap.NewFromStringMap(raw).Unmarshal(fromDocs))
	def := createDefaultConfig().(*Config)
	assert.Equal(t, def, fromDocs)
}
