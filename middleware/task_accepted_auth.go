package middleware

import (
	"errors"
	"net/http"

	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// TokenAuthForAcceptedTask permits a depleted or expired token to observe its
// own native accepted task. New work and legacy task reads still pass strict
// TokenAuth. Disabled tokens, IP restrictions and banned users always fail.
func TokenAuthForAcceptedTask(taskParam string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !authenticateAcceptedTaskTokenReadOnly(c) {
			return
		}
		task, exists, err := model.GetByTaskId(c.GetInt("id"), c.Param(taskParam))
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		if exists {
			_, err = model.GetTaskIntake(c.Request.Context(), task.ID)
			if err == nil {
				c.Next()
				return
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				c.AbortWithStatus(http.StatusServiceUnavailable)
				return
			}
		}
		TokenAuth()(c)
	}
}
