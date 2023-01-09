package transport

import (
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"github.com/yusufsyaifudin/khook/internal/svc/resourcesvc"
	"github.com/yusufsyaifudin/khook/pkg/respbuilder"
	"net/http"
	"runtime"
)

const (
	RoutePutResource              = "/resources"
	RouteGetRegisteredWebhooks    = "/webhooks"
	RouteAddRegisteredWebhooks    = "/webhooks/{label}"
	RoutePauseRegisteredWebhooks  = "/webhooks/{label}/pause"
	RouteResumeRegisteredWebhooks = "/webhooks/{label}/resume"

	RouteGetStatsSystem         = "/stats/system"
	RouteGetStatsActiveKafka    = "/stats/active-kafka"
	RouteGetStatsActiveWebhooks = "/stats/active-webhooks"
	RouteGetStatsPausedWebhooks = "/stats/paused-webhooks"
)

type HttpCfg struct {
	ResourceSvc resourcesvc.ResourceService
}

type HTTP struct {
	router chi.Router
	Cfg    HttpCfg
}

var _ http.Handler = (*HTTP)(nil)

func NewHTTP(cfg HttpCfg) *HTTP {
	chiMux := chi.NewMux()
	// register middleware, if any...

	svc := &HTTP{
		router: chiMux,
		Cfg:    cfg,
	}

	svc.registerRoutes()
	return svc
}

func (h *HTTP) registerRoutes() {
	h.router.Get(RouteGetRegisteredWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		outGetWebhooks, err := h.Cfg.ResourceSvc.GetConsumers(ctx)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outGetWebhooks)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Get(RouteGetStatsSystem, func(w http.ResponseWriter, r *http.Request) {
		var bToMb = func(b uint64) uint64 {
			return b / 1024 / 1024
		}

		ctx := r.Context()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		stats := map[string]any{
			"num_gc":      m.NumGC,
			"alloc":       bToMb(m.Alloc),
			"total_alloc": bToMb(m.TotalAlloc),
			"heap_alloc":  bToMb(m.HeapAlloc),
			"sys":         bToMb(m.Sys),
		}

		respBody := respbuilder.Success(ctx, stats)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Get(RouteGetStatsActiveKafka, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		conn := h.Cfg.ResourceSvc.GetActiveKafkaConfigs(ctx)
		respBody := respbuilder.Success(ctx, conn)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Put(RoutePutResource, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var reqBody resourcesvc.InAddResource
		bodyDec := json.NewDecoder(r.Body)
		err := bodyDec.Decode(&reqBody)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		outAdd, err := h.Cfg.ResourceSvc.AddResource(ctx, reqBody)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outAdd)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Get(RouteGetStatsActiveWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		activeWebhooks, err := h.Cfg.ResourceSvc.GetActiveConsumers(ctx)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, activeWebhooks)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})
}

func (h *HTTP) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.router.ServeHTTP(writer, request)
}
