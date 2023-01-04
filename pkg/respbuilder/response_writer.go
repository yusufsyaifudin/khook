package respbuilder

import (
	"encoding/json"
	"net/http"
)

func WriteJSON(httpStatus int, rw http.ResponseWriter, r *http.Request, data interface{}) {
	tracer := MustExtract(r.Context())

	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Tracer-ID", tracer.AppTraceID)
	rw.WriteHeader(httpStatus)

	enc := json.NewEncoder(rw)
	err := enc.Encode(data)
	if err != nil {
		reason := ReasonMap[ErrValidation]
		errPayload, _ := json.Marshal(HTTPError{
			Err: ErrorEntity{
				Code:    reason.Code,
				Message: reason.Message,
				Debug:   err.Error(),
				TraceID: tracer.AppTraceID,
			},
		})

		_, _ = rw.Write(errPayload)
		return
	}

	return
}
