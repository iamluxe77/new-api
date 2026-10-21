package controller

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func GetTaskRecoveryStatus(c *gin.Context) {
	stages, err := model.TaskRecoveryAggregates(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "task recovery status unavailable"})
		return
	}
	var last model.SystemTask
	err = model.DB.WithContext(c.Request.Context()).Select("id", "updated_at").
		Where("type = ? AND status = ?", model.SystemTaskTypeAsyncTaskPoll, model.SystemTaskStatusSucceeded).
		Order("id desc").Limit(1).Find(&last).Error
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "task recovery status unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stages": stages,
		"last_successful_recovery_pass": last.UpdatedAt})
}

func GetBlockedTaskRecovery(c *gin.Context) {
	view, err := model.GetBlockedTaskDispatchStatus(c.Request.Context(), c.Query("task_id"))
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, model.ErrTaskDispatchConflict) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": "task recovery status unavailable"})
		return
	}
	c.JSON(http.StatusOK, view)
}

func ResumeBlockedTaskRecovery(c *gin.Context) {
	var request struct {
		TaskID          string `json:"task_id" binding:"required"`
		ExpectedVersion int64  `json:"expected_version" binding:"required"`
		Reason          string `json:"reason" binding:"required"`
	}
	if c.ShouldBindJSON(&request) != nil || c.GetInt("id") <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery request"})
		return
	}
	view, err := model.GetBlockedTaskDispatchStatus(c.Request.Context(), request.TaskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task recovery status unavailable"})
		return
	}
	if view.Phase != model.TaskDispatchBlocked || view.Version != request.ExpectedVersion || !view.Reserved {
		c.JSON(http.StatusConflict, gin.H{"error": "task recovery version conflict"})
		return
	}
	var task model.Task
	if err := model.DB.WithContext(c.Request.Context()).Select("id").Where("task_id = ?", request.TaskID).Take(&task).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "task recovery unavailable"})
		return
	}
	if err := model.ResumeBlockedTaskDispatch(c.Request.Context(), task.ID, request.ExpectedVersion, request.Reason, c.GetInt("id")); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "task recovery version conflict"})
		return
	}
	recordManageAudit(c, "task.recovery_resume", map[string]any{"task_id": request.TaskID,
		"version": request.ExpectedVersion + 1, "reason": request.Reason})
	c.JSON(http.StatusOK, gin.H{"task_id": request.TaskID, "phase": view.BlockedFrom,
		"version": strconv.FormatInt(request.ExpectedVersion+1, 10)})
}
