package respbuilder

type ErrKind int64

const (
	ErrUnhandled ErrKind = iota + 1
	ErrValidation
	ErrDuplicateEntries
	ErrResourceNotFound
	ErrUnauthorized
)

type Reason struct {
	Code    string
	Message string
}

func (r *Reason) Error() string {
	return r.Message
}

var ErrX = &Reason{Code: "", Message: ""}

var ReasonMap = map[ErrKind]Reason{
	ErrUnhandled:        {Code: "01", Message: "unhandled error"},
	ErrValidation:       {Code: "02", Message: "error validation"},
	ErrDuplicateEntries: {Code: "03", Message: "duplicate entries"},
	ErrResourceNotFound: {Code: "04", Message: "resource not found"},
	ErrUnauthorized:     {Code: "05", Message: "unauthorized"},
}

// ErrorEntity contain code, message, debug (*if applicable) and trace id.
type ErrorEntity struct {
	Code    string `json:"error_code"`        // to handle by FE
	Message string `json:"error_description"` // to handle by FE (string version of the error code)
	Debug   string `json:"debug,omitempty"`   // technical error
	TraceID string `json:"trace_id"`
}

// HTTPError follow Facebook error response object:
// https://developers.facebook.com/docs/graph-api/using-graph-api/error-handling/
// {"error":{"message":"Message describing the error","type":"OAuthException",
// "code":190,"error_subcode":460,"error_user_title":"A title","error_user_msg":"A message",
// "fbtrace_id":"EJplcsCHuLu"}}
type HTTPError struct {
	Err ErrorEntity `json:"error"`
}

func (e HTTPError) Error() string {
	return e.Err.Message + ": " + e.Err.Debug
}

// HTTPSuccess success response always wrap in data key.
type HTTPSuccess struct {
	TraceID string      `json:"trace_id"`
	Data    interface{} `json:"data"`
}
