package respbuilder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

func Error(ctx context.Context, reasonKind ErrKind, err error) HTTPError {
	stuff := MustExtract(ctx)

	errMsg := ""
	if err != nil {
		var errJson *json.UnmarshalTypeError
		if errors.As(err, &errJson) {
			// TODO: handle specific type error here
		}

		errMsg = err.Error()
	}

	err = fmt.Errorf("%v %w", err, ErrX)

	reason, ok := ReasonMap[reasonKind]
	if !ok {
		return HTTPError{
			Err: ErrorEntity{
				Code:    "XX",
				Message: "unknown error kind",
				Debug:   "", // don't show message if unknown type, to prevent security breach
				TraceID: stuff.AppTraceID,
			},
		}
	}

	return HTTPError{
		Err: ErrorEntity{
			Code:    reason.Code,
			Message: reason.Message,
			Debug:   errMsg,
			TraceID: stuff.AppTraceID,
		},
	}
}

func Success(ctx context.Context, data interface{}) HTTPSuccess {
	stuff := MustExtract(ctx)

	return HTTPSuccess{
		TraceID: stuff.AppTraceID,
		Data:    data,
	}
}
