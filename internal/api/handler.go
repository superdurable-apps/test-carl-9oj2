package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/superdurable/dex-template-basic-process/internal/api/generated"
	"github.com/superdurable/dex-template-basic-process/internal/process"
	"github.com/superdurable/dex/sdk-go/dex"
)

// Handler maps the OpenAPI contract to the authoritative HotspotRadarFlow.
type Handler struct{ client *dex.Client }

func NewHandler(client *dex.Client) (*generated.Server, error) {
	return generated.NewServer(&Handler{client: client},
		generated.WithErrorHandler(writeGeneratedError),
		generated.WithNotFound(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusNotFound, "not_found", "unknown API route")
		}),
		generated.WithMethodNotAllowed(func(w http.ResponseWriter, _ *http.Request, allowed string) {
			w.Header().Set("Allow", allowed)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		}),
	)
}

func (*Handler) NewError(ctx context.Context, err error) *generated.ErrorStatusCode {
	slog.ErrorContext(ctx, "API handler failed", "error", process.ErrorTail(err))
	return &generated.ErrorStatusCode{StatusCode: http.StatusInternalServerError,
		Response: generated.ErrorResponse{Error: "application_unavailable", Message: "application is unavailable"}}
}
func (*Handler) GetHealth(context.Context) (*generated.HealthResponse, error) {
	return &generated.HealthResponse{Status: generated.HealthResponseStatusOk}, nil
}
func (handler *Handler) RequestRadarRun(ctx context.Context, request *generated.RadarRunRequest) (*generated.RadarRunAccepted, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := process.RequestRadarRun(ctx, handler.client, request.RequestId.String()); err != nil {
		return nil, err
	}
	return &generated.RadarRunAccepted{RequestId: request.RequestId}, nil
}
func (handler *Handler) GetRadar(ctx context.Context) (*generated.RadarSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	snapshot, err := process.ReadRadar(ctx, handler.client)
	if err != nil {
		return nil, err
	}
	hotspots := make([]generated.RadarHotspot, 0, len(snapshot.Hotspots))
	for _, h := range snapshot.Hotspots {
		evidence := make([]generated.RadarEvidence, 0, len(h.Evidence))
		for _, e := range h.Evidence {
			evidence = append(evidence, generated.RadarEvidence{
				ID: e.ID, Title: e.Title, Text: e.Text, Author: e.Author, Score: e.Score, URL: e.URL, ObservedAt: e.ObservedAt,
			})
		}
		hotspots = append(hotspots, generated.RadarHotspot{Name: h.Name, Concept: h.Concept, Primitives: h.Primitives,
			PrimitivesEn: h.PrimitivesEN, SearchTerms: h.SearchTerms, Domain: generated.RadarHotspotDomain(h.Domain),
			AuthorCount: h.AuthorCount, FirstSeen: h.FirstSeen, Evidence: evidence})
	}
	return &generated.RadarSnapshot{RoundNumber: snapshot.RoundNumber, Status: snapshot.Status, LastRunAt: snapshot.LastRunAt, NextRunAt: snapshot.NextRunAt,
		NewHotspots: snapshot.NewHotspots, NoiseCount: snapshot.NoiseCount, PendingCount: snapshot.PendingCount, SourceStatus: snapshot.SourceStatus,
		Report: snapshot.Report, HTML: snapshot.HTML, Hotspots: hotspots}, nil
}
func (*Handler) BacktestRadar(_ context.Context, request *generated.BacktestRequest) (*generated.BacktestResult, error) {
	if strings.TrimSpace(request.KnownHotspot) == "" || len(request.Items) == 0 || len(request.Items) > 500 || request.WidespreadAt.IsZero() {
		return nil, apiError(http.StatusBadRequest, "invalid_backtest", "Provide a hotspot, broad-discussion time and 1 to 500 items.")
	}
	items := make([]process.HistoricalItem, 0, len(request.Items))
	for _, item := range request.Items {
		if item.Time.IsZero() || strings.TrimSpace(item.Title) == "" || item.Score < 0 {
			return nil, apiError(http.StatusBadRequest, "invalid_backtest", "Every item needs a time, title and nonnegative score.")
		}
		items = append(items, process.HistoricalItem{Time: item.Time, Title: item.Title, Text: item.Text, Author: item.Author, Score: item.Score})
	}
	outcome := process.Backtest(items, request.KnownHotspot, request.WidespreadAt)
	return &generated.BacktestResult{Detected: outcome.Detected, DetectedAt: outcome.DetectedAt,
		LeadHours: outcome.LeadHours, Interpretation: outcome.Interpretation}, nil
}
func apiError(status int, code, message string) *generated.ErrorStatusCode {
	return &generated.ErrorStatusCode{StatusCode: status, Response: generated.ErrorResponse{Error: code, Message: message}}
}
func writeGeneratedError(_ context.Context, w http.ResponseWriter, _ *http.Request, _ error) {
	writeError(w, http.StatusBadRequest, "invalid_request", "request does not match the OpenAPI contract")
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(generated.ErrorResponse{Error: code, Message: message})
}
