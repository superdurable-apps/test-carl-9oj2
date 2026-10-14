//go:build apptest

package apptest_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex-connectors-library/connectors/hackernews"
	"github.com/superdurable/dex-connectors-library/connectors/superdurable/llm"
	"github.com/superdurable/dex-connectors-library/connectors/superdurable/llm/llmmock"
	"github.com/superdurable/dex-connectors-library/sdkgo"
	"github.com/superdurable/dex-connectors-library/sdkgo/connectormock"
	"github.com/superdurable/dex-template-basic-process/internal/api/generated"
	"github.com/superdurable/dex-template-basic-process/internal/apptest"
	"github.com/superdurable/dex-template-basic-process/internal/process"
	"github.com/superdurable/dex-template-basic-process/internal/worker"
)

type newsOperations struct {
	feeds *connectormock.Query[hackernews.ListStoryIDsInput, hackernews.StoryIDs]
	items *connectormock.Query[hackernews.GetItemInput, hackernews.Item]
}

func (m newsOperations) ListStoryIDs() sdkgo.Query[hackernews.ListStoryIDsInput, hackernews.StoryIDs] {
	return m.feeds
}
func (m newsOperations) GetItem() sdkgo.Query[hackernews.GetItemInput, hackernews.Item] {
	return m.items
}

func newsMock(t *testing.T) (hackernews.Connection, newsOperations) {
	t.Helper()
	m := newsOperations{
		feeds: connectormock.NewQuery[hackernews.ListStoryIDsInput, hackernews.StoryIDs](t, hackernews.ListStoryIDsDefinition),
		items: connectormock.NewQuery[hackernews.GetItemInput, hackernews.Item](t, hackernews.GetItemDefinition),
	}
	connection, err := hackernews.NewConnectionWithOperations(m, sdkgo.ConnectionRef{Provider: "hacker-news", Name: process.HackerNewsConnectionName})
	if err != nil {
		t.Fatal(err)
	}
	return connection, m
}
func radarEnv(t *testing.T) (*apptest.Env, newsOperations, *llmmock.Mock) {
	t.Helper()
	news, ops := newsMock(t)
	model := llmmock.New(t, process.RadarModelConnectionName)
	env := apptest.Start(t, worker.Connections{HackerNews: news, RadarModel: model.Connection()})
	return env, ops, model
}
func hotspotAnswer() connectormock.Case[llm.GenerateTextResponse] {
	return llmmock.GenerateTextGenerated(llm.GenerateTextResponse{Text: `{"kind":"hotspot","name":"Claude Dashboards","concept":"Dashboards for AI teams","primitives":["看板"],"primitives_en":["dashboard"],"search_terms":["Claude Dashboards"],"domain":"general"}`}, nil)
}
func modelTerms(calls []connectormock.Call[llm.GenerateTextRequest]) map[string]bool {
	terms := map[string]bool{}
	for _, call := range calls {
		if len(call.Input.Messages) == 0 {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(call.Input.Messages[0].Text), &payload) != nil {
			continue
		}
		term, _ := payload["term"].(string)
		terms[term] = true
	}
	return terms
}
func queueStories(ops newsOperations, firstID int64) {
	queueNamedStories(ops, firstID, "Claude Dashboards launches for AI teams")
}
func queueNamedStories(ops newsOperations, firstID int64, title string) {
	ops.feeds.Respond(
		connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{IDs: []int64{firstID, firstID + 1}}, nil),
		connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{}, nil),
	)
	for i := int64(0); i < 2; i++ {
		id := firstID + i
		item := hackernews.Item{ID: id, Type: "story", By: fmt.Sprintf("author-%d", id), Time: time.Now().UTC().Unix(),
			Title: title, Score: 60, DiscussionURL: fmt.Sprintf("https://news.ycombinator.com/item?id=%d", id)}
		ops.items.Respond(connectormock.Branch(hackernews.GetItemBranchFound, item, nil))
	}
}
func queueFeedFailure(ops newsOperations, branch sdkgo.BranchID, kind sdkgo.FailureKind) {
	ops.feeds.Respond(connectormock.Branch(branch, hackernews.StoryIDs{}, &sdkgo.Failure{Kind: kind, Message: "feed failed"}),
		connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{}, nil))
}
func queueItemFailure(ops newsOperations, id int64, branch sdkgo.BranchID, kind sdkgo.FailureKind) {
	ops.feeds.Respond(connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{IDs: []int64{id}}, nil),
		connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{}, nil))
	ops.items.Respond(connectormock.Branch(branch, hackernews.Item{}, &sdkgo.Failure{Kind: kind, Message: "item failed"}))
}
func waitRadar(t *testing.T, env *apptest.Env, previous int) *generated.RadarSnapshot {
	t.Helper()
	var snapshot *generated.RadarSnapshot
	env.Eventually(t, 30*time.Second, func() bool {
		read, err := env.API.GetRadar(env.Context(t))
		if err != nil {
			t.Fatalf("read radar: %v", err)
		}
		snapshot = read
		return read.RoundNumber > previous && read.Status == "ready"
	})
	return snapshot
}
func requestRound(t *testing.T, env *apptest.Env, id uuid.UUID) {
	t.Helper()
	accepted, err := env.API.RequestRadarRun(env.Context(t), &generated.RadarRunRequest{RequestId: id})
	if err != nil || accepted.RequestId != id {
		t.Fatalf("request radar round: %+v %v", accepted, err)
	}
}
func runRadar(t *testing.T, env *apptest.Env, previous int) *generated.RadarSnapshot {
	t.Helper()
	requestRound(t, env, uuid.New())
	return waitRadar(t, env, previous)
}

func TestRadarRoundsAndBranches(t *testing.T) {
	env, ops, model := radarEnv(t)
	model.GenerateText().Default(llmmock.GenerateTextGenerated(llm.GenerateTextResponse{Text: "not json"}, nil))
	queueStories(ops, 1)
	snapshot := runRadar(t, env, 0)
	if snapshot.NewHotspots != 0 || len(snapshot.Hotspots) != 0 || snapshot.PendingCount == 0 {
		t.Fatalf("invented hotspot from invalid verdict: %+v", snapshot)
	}

	model.GenerateText().Default(hotspotAnswer())
	queueStories(ops, 3)
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	if snapshot.NewHotspots != 1 || len(snapshot.Hotspots) != 1 {
		t.Fatalf("want one hotspot: %+v", snapshot)
	}
	h := snapshot.Hotspots[0]
	if h.Name != "Claude Dashboards" || h.Concept == "" || len(h.Primitives) == 0 || len(h.Evidence) != 2 {
		t.Fatalf("incomplete hotspot: %+v", h)
	}
	if !strings.Contains(snapshot.HTML, "<meta name='viewport'") || !strings.Contains(snapshot.HTML, "<section data-group='今天'><h2>今天</h2>") || !strings.Contains(snapshot.HTML, "Claude Dashboards") || !strings.Contains(snapshot.HTML, "Hacker News") {
		t.Fatalf("dashboard missing embedded data")
	}
	calls := len(model.GenerateText().Calls())
	reused := uuid.New()
	// The queued request itself is one round, but submitting it again adds none.
	queueStories(ops, 3)
	requestRound(t, env, reused)
	snapshot = waitRadar(t, env, snapshot.RoundNumber)
	requestRound(t, env, reused)
	if len(model.GenerateText().Calls()) != calls || snapshot.NewHotspots != 1 || len(snapshot.Hotspots) != 1 {
		t.Fatalf("duplicate stories or request repeated judgement: %+v", snapshot)
	}

	queueFeedFailure(ops, hackernews.ListStoryIDsBranchProviderRejected, sdkgo.FailureProviderRejection)
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	if snapshot.SourceStatus == "Hacker News: successful" {
		t.Fatal("source failure hidden")
	}
	queueFeedFailure(ops, hackernews.ListStoryIDsBranchInvalidResponse, sdkgo.FailureProtocol)
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	queueFeedFailure(ops, hackernews.ListStoryIDsBranchDefect, sdkgo.FailureValidation)
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	itemBranches := []struct {
		branch sdkgo.BranchID
		kind   sdkgo.FailureKind
	}{
		{hackernews.GetItemBranchNotFound, sdkgo.FailureNotFound},
		{hackernews.GetItemBranchProviderRejected, sdkgo.FailureProviderRejection},
		{hackernews.GetItemBranchInvalidResponse, sdkgo.FailureProtocol},
		{hackernews.GetItemBranchDefect, sdkgo.FailureValidation},
	}
	for i, test := range itemBranches {
		queueItemFailure(ops, int64(100+i), test.branch, test.kind)
		snapshot = runRadar(t, env, snapshot.RoundNumber)
	}

	modelBranches := []connectormock.Case[llm.GenerateTextResponse]{
		llmmock.GenerateTextOutputTokenLimit(), llmmock.GenerateTextContentPolicy(),
		llmmock.GenerateTextInvalidKey(), llmmock.GenerateTextSchemaMismatch(),
		llmmock.GenerateTextUnsupportedField(),
	}
	for i, answer := range modelBranches {
		model.GenerateText().Default(answer)
		queueStories(ops, int64(200+i*2))
		snapshot = runRadar(t, env, snapshot.RoundNumber)
		if !strings.Contains(snapshot.Report, "New hotspots: 0") {
			t.Fatalf("failure branch invented a hotspot: %+v", snapshot)
		}
	}
	model.GenerateText().Default(llmmock.GenerateTextGenerated(llm.GenerateTextResponse{Text: `{"kind":"known"}`}, nil))
	queueNamedStories(ops, 400, "Neural Orchard AI workspace")
	priorCalls := len(model.GenerateText().Calls())
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	if snapshot.NoiseCount == 0 {
		t.Fatal("known terms were not remembered")
	}
	afterKnownCalls := len(model.GenerateText().Calls())
	remembered := modelTerms(model.GenerateText().Calls()[priorCalls:afterKnownCalls])
	queueNamedStories(ops, 402, "Neural Orchard AI workspace")
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	for term := range modelTerms(model.GenerateText().Calls()[afterKnownCalls:]) {
		if remembered[term] {
			t.Fatalf("known term %q was sent to the model again", term)
		}
	}
	model.GenerateText().Default(llmmock.GenerateTextGenerated(llm.GenerateTextResponse{Text: `{"kind":"noise"}`}, nil))
	queueNamedStories(ops, 500, "Quantum Canvas AI editor")
	snapshot = runRadar(t, env, snapshot.RoundNumber)
	if snapshot.NoiseCount == 0 {
		t.Fatal("noise terms were not remembered")
	}
	if len(ops.feeds.Calls()) != 2*snapshot.RoundNumber {
		t.Fatalf("missing feed calls: %d for %d rounds", len(ops.feeds.Calls()), snapshot.RoundNumber)
	}
}

func TestRadarBacktestBlindTimingAndInput(t *testing.T) {
	env, ops, _ := radarEnv(t)
	ops.feeds.Default(connectormock.Branch(hackernews.ListStoryIDsBranchListed, hackernews.StoryIDs{}, nil))
	start := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	request := &generated.BacktestRequest{
		KnownHotspot: "Claude Dashboards", WidespreadAt: start.Add(24 * time.Hour),
		Items: []generated.BacktestItem{
			{Time: start, Title: "Claude Dashboards launches", Author: "a", Score: 20},
			{Time: start.Add(2 * time.Hour), Title: "Claude Dashboards preview", Author: "b", Score: 20},
		},
	}
	result, err := env.API.BacktestRadar(env.Context(t), request)
	if err != nil || !result.Detected || result.LeadHours != 22 || result.Interpretation != "early" {
		t.Fatalf("backtest: %+v %v", result, err)
	}
	request.KnownHotspot = "Unknown Product"
	result, err = env.API.BacktestRadar(env.Context(t), request)
	if err != nil || result.Detected {
		t.Fatalf("unknown hotspot detected: %+v %v", result, err)
	}
	request.Items = nil
	_, err = env.API.BacktestRadar(env.Context(t), request)
	var apiErr *generated.ErrorStatusCode
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid backtest: %v", err)
	}
}
