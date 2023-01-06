package transport

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/yusufsyaifudin/khook/internal/svc/resourcesvc"
	"github.com/yusufsyaifudin/khook/pkg/respbuilder"
	"github.com/yusufsyaifudin/khook/storage"
	"net/http"
	"runtime"
)

const (
	RouteGetRegisteredWebhooks    = "/webhooks"
	RouteAddRegisteredWebhooks    = "/webhooks/{label}"
	RoutePauseRegisteredWebhooks  = "/webhooks/{label}/pause"
	RouteResumeRegisteredWebhooks = "/webhooks/{label}/resume"
	RoutePutKafka                 = "/kafka/{label}"

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

		outGetWebhooks, err := h.Cfg.ResourceSvc.GetWebhooks(ctx)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outGetWebhooks)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Put(RouteAddRegisteredWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var reqBody storage.SinkTarget
		bodyDec := json.NewDecoder(r.Body)
		err := bodyDec.Decode(&reqBody)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		reqBody.Label = chi.URLParam(r, "label")
		if reqBody.Label == "" {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, fmt.Errorf("empty label"))
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		outAdd, err := h.Cfg.ResourceSvc.AddWebhook(ctx, resourcesvc.InputAddWebhook{Webhook: reqBody})
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outAdd)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Get(RoutePauseRegisteredWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		webhookLabel := chi.URLParam(r, "label")
		if webhookLabel == "" {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, fmt.Errorf("empty label"))
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		outPause, err := h.Cfg.ResourceSvc.PauseWebhook(ctx, resourcesvc.InPauseWebhook{Label: webhookLabel})
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outPause)
		respbuilder.WriteJSON(http.StatusOK, w, r, respBody)
		return
	})

	h.router.Get(RouteResumeRegisteredWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		webhookLabel := chi.URLParam(r, "label")
		if webhookLabel == "" {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, fmt.Errorf("empty label"))
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		outResume, err := h.Cfg.ResourceSvc.ResumeWebhook(ctx, resourcesvc.InResumeWebhook{Label: webhookLabel})
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		respBody := respbuilder.Success(ctx, outResume)
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

	h.router.Put(RoutePutKafka, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var reqBody resourcesvc.InAddKafkaConfig
		bodyDec := json.NewDecoder(r.Body)
		err := bodyDec.Decode(&reqBody)
		if err != nil {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, err)
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		reqBody.Label = chi.URLParam(r, "label")
		if reqBody.Label == "" {
			respBody := respbuilder.Error(ctx, respbuilder.ErrValidation, fmt.Errorf("empty label"))
			respbuilder.WriteJSON(http.StatusUnprocessableEntity, w, r, respBody)
			return
		}

		outAdd, err := h.Cfg.ResourceSvc.AddKafkaConfig(ctx, reqBody)
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

	h.router.Get(RouteGetStatsPausedWebhooks, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		activeWebhooks, err := h.Cfg.ResourceSvc.GetPausedWebhooks(ctx)
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
