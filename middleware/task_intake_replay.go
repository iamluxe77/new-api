package middleware

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

const taskOriginalRequestKey = "task_original_request"

// TaskIntakeReplay runs before current plugin routing, rate limits and channel
// selection. A read-only token identity can only return an already accepted
// task; an absent key or task still enters the normal strict TokenAuth chain.
func TaskIntakeReplay(present func(*gin.Context, *model.Task)) gin.HandlerFunc {
	return func(c *gin.Context) {
		if TryTaskIntakeReplay(c, present) {
			c.Next()
		}
	}
}

// TryTaskIntakeReplay does not advance Gin's handler chain. The outer plugin
// dispatcher uses it before deciding whether a current route exists.
func TryTaskIntakeReplay(c *gin.Context, present func(*gin.Context, *model.Task)) bool {
	if _, captured := OriginalTaskRequest(c); captured {
		return true
	}
	key := c.GetHeader("Idempotency-Key")
	otherKey := c.GetHeader("X-Idempotency-Key")
	if key == "" {
		key = otherKey
	}
	if key == "" {
		return true
	}
	if !authenticateAcceptedTaskTokenReadOnly(c) {
		return false
	}
	exists, err := model.HasOwnedTaskIntakeClientKey(c.Request.Context(), c.GetInt("id"), c.GetInt("token_id"), key)
	if err != nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return false
	}
	if otherKey != "" && otherKey != key {
		otherExists, otherErr := model.HasOwnedTaskIntakeClientKey(c.Request.Context(), c.GetInt("id"), c.GetInt("token_id"), otherKey)
		if otherErr != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return false
		}
		exists = exists || otherExists
	}
	if !exists {
		return true
	}
	if key != otherKey && otherKey != "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "conflicting idempotency keys", "type": "invalid_request_error"}})
		return false
	}
	original, ok := captureTaskOriginalRequest(c, key, true)
	if !ok {
		return false
	}
	claim, err := model.FindTaskIntakeByClientKey(c.Request.Context(), c.GetInt("id"), c.GetInt("token_id"), original)
	if err != nil {
		status, code := http.StatusServiceUnavailable, "task_intake_unavailable"
		if errors.Is(err, model.ErrTaskIntakeConflict) {
			status, code = http.StatusConflict, "idempotency_key_conflict"
		} else if errors.Is(err, model.ErrTaskIntakeInvalid) || errors.Is(err, model.ErrTaskIntakeTooLarge) {
			status, code = http.StatusBadRequest, "invalid_request"
		}
		errorType := "server_error"
		if status < http.StatusInternalServerError {
			errorType = "invalid_request_error"
		}
		c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": code, "type": errorType, "code": code}})
		return false
	}
	if claim != nil && claim.Task != nil {
		present(c, claim.Task)
		c.Abort()
		return false
	}
	return true
}

// CaptureTaskOriginalRequest runs after strict TokenAuth for keyless creates.
func CaptureTaskOriginalRequest() gin.HandlerFunc {
	return func(c *gin.Context) {
		if os.Getenv("TASK_DURABLE_SUBMIT_ENABLED") != "true" {
			c.Next()
			return
		}
		if _, captured := OriginalTaskRequest(c); !captured {
			key := c.GetHeader("Idempotency-Key")
			otherKey := c.GetHeader("X-Idempotency-Key")
			if key == "" {
				key = otherKey
			}
			if _, ok := captureTaskOriginalRequest(c, key, false); !ok {
				return
			}
		}
		c.Next()
	}
}

func captureTaskOriginalRequest(c *gin.Context, key string, durableLimit bool) (model.TaskOriginalRequest, bool) {
	if c.Request.Body == nil {
		c.Request.Body = http.NoBody
	}
	c.Set(common.KeyMemoryOnlyBody, true)
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		status := http.StatusBadRequest
		if common.IsRequestBodyTooLargeError(err) {
			status = http.StatusRequestEntityTooLarge
		}
		c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": "cannot read task request body", "type": "invalid_request_error"}})
		return model.TaskOriginalRequest{}, false
	}
	if durableLimit && storage.Size() > model.MaxTaskRawInputBytes {
		c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": gin.H{"message": "task request body is too large", "type": "invalid_request_error"}})
		return model.TaskOriginalRequest{}, false
	}
	raw, err := storage.Bytes()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "cannot read task request body", "type": "invalid_request_error"}})
		return model.TaskOriginalRequest{}, false
	}
	if _, err := storage.Seek(0, io.SeekStart); err != nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return model.TaskOriginalRequest{}, false
	}
	c.Request.Body = io.NopCloser(storage)
	// Plugin submit and pricing hooks receive Content-Type and Accept only.
	// Content-Type is fingerprinted separately; proxy and trace headers vary
	// between otherwise identical client retries.
	headers := make(map[string]string, 1)
	if value := strings.TrimSpace(c.GetHeader("Accept")); value != "" {
		headers["accept"] = value
	}
	original := model.TaskOriginalRequest{
		ClientKey: key, Method: c.Request.Method, Path: c.Request.URL.Path,
		RawQuery: c.Request.URL.RawQuery, ContentType: c.GetHeader("Content-Type"),
		RawInput: raw, SemanticHeaders: headers,
	}
	c.Set(taskOriginalRequestKey, original)
	return original, true
}

func OriginalTaskRequest(c *gin.Context) (model.TaskOriginalRequest, bool) {
	value, ok := c.Get(taskOriginalRequestKey)
	original, valid := value.(model.TaskOriginalRequest)
	return original, ok && valid
}
