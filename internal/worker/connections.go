package worker

import (
	"fmt"

	"github.com/superdurable/dex-connectors-library/connectors/hackernews"
	"github.com/superdurable/dex-connectors-library/connectors/superdurable/llm"
	"github.com/superdurable/dex-template-basic-process/internal/connectorconfiguration"
	"github.com/superdurable/dex-template-basic-process/internal/process"
)

// Connections holds the project's public Hacker News and configured model connections.
type Connections struct {
	HackerNews hackernews.Connection
	RadarModel llm.Connection
}

func OpenConnections(configuration connectorconfiguration.Configuration) (Connections, error) {
	news, err := hackernews.NewProjectConnection(configuration.Project(), process.HackerNewsConnectionName)
	if err != nil {
		return Connections{}, fmt.Errorf("configure %s connection: %w", process.HackerNewsConnectionName, err)
	}
	model, err := llm.NewProjectConnection(configuration.Project(), process.RadarModelConnectionName)
	if err != nil {
		return Connections{}, fmt.Errorf("configure %s connection: %w", process.RadarModelConnectionName, err)
	}
	return Connections{HackerNews: news, RadarModel: model}, nil
}
