package controller

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	pluginruntime "github.com/QuantumNous/new-api/pkg/jsplugin"
	jspluginadaptor "github.com/QuantumNous/new-api/relay/channel/task/jsplugin"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// TryPresentDurableNativeQuery matches an exact accepted query path before the
// mutable plugin router. Unmatched requests retain the normal strict route.
func TryPresentDurableNativeQuery(c *gin.Context) bool {
	binding, err := model.FindTaskIntakeQueryRoute(c.Request.Context(), c.Request.Method, c.Request.URL.Path)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false
	}
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	if !middleware.AuthenticateAcceptedTaskTokenReadOnly(c) {
		return true
	}
	var task model.Task
	if err := model.DB.WithContext(c.Request.Context()).First(&task, binding.TaskID).Error; err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	if task.UserId != c.GetInt("id") || !task.ResultRetrievable() {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"message": "task not found", "type": "invalid_request_error", "code": "task_not_found"}})
		return true
	}
	if c.Request.ContentLength > 0 || len(c.Request.TransferEncoding) > 0 {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": gin.H{"message": "unsupported request body for this operation", "type": "invalid_request_error"}})
		return true
	}
	intake, plugin, err := service.LoadFrozenTaskPlugin(c.Request.Context(), task.ID)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	allowed, err := durableTaskQueryRoutes(plugin, task.TaskID)
	if err != nil || intake.UserID != task.UserId || string(task.Platform) != intake.PluginKey {
		presentFrozenTaskUnavailable(c)
		return true
	}
	matched := false
	for _, route := range allowed {
		if route.Path == binding.Path && route.Render == binding.Render && route.TaskIDParam == binding.TaskIDParam {
			matched = true
			break
		}
	}
	if !matched {
		presentFrozenTaskUnavailable(c)
		return true
	}
	view, err := service.BuildDurableTaskPluginView(c.Request.Context(), &task)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	encoded, err := common.Marshal(view)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	request := pluginruntime.RouteRequestContext{
		Path: c.Request.URL.Path, Method: c.Request.Method,
		Params: map[string]string{binding.TaskIDParam: task.TaskID},
		Query:  c.Request.URL.Query(), Body: map[string]any{"kind": string(pluginruntime.BodyNone)},
	}
	result, err := plugin.Engine.CallPath(c.Request.Context(), "native", []string{binding.Render}, request.JSValue(), pluginruntime.RawJSON(encoded))
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	c.JSON(http.StatusOK, result)
	return true
}

// TryPresentDurableTaskFetch handles accepted native video reads without
// consulting today's channel or plugin registry. Legacy tasks continue into
// their existing Distribute/RelayTaskFetch chain.
func TryPresentDurableTaskFetch(c *gin.Context) bool {
	taskID := c.Param("task_id")
	if taskID == "" {
		return false
	}
	task, exists, err := model.GetByTaskId(c.GetInt("id"), taskID)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	if !exists {
		return false
	}
	_, err = model.GetTaskIntake(c.Request.Context(), task.ID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false
	}
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return true
	}
	if !task.ResultRetrievable() {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"message": "task not found", "type": "invalid_request_error", "code": "task_not_found"}})
		return true
	}
	PresentDurableTaskReplay(c, task)
	return true
}

// PresentDurableTaskReplay uses only the accepted task's private snapshot. It
// is safe to call before current routing, price lookup or rate limiting.
func PresentDurableTaskReplay(c *gin.Context, task *model.Task) {
	if task == nil || task.UserId != c.GetInt("id") {
		presentFrozenTaskUnavailable(c)
		return
	}
	intake, plugin, err := service.LoadFrozenTaskPlugin(c.Request.Context(), task.ID)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	routing, err := service.DecodeDurableRoutingSnapshot(intake)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	deliveryTask, outcome, err := service.DurableTaskDelivery(c.Request.Context(), task, routing.RetainResult)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	task = deliveryTask
	var billing service.DurableTaskBillingSnapshot
	if common.Unmarshal(intake.BillingSnapshot, &billing) == nil {
		ratios := billing.Context.OtherRatios
		if ratios == nil {
			ratios = map[string]float64{}
		}
		if encoded, marshalErr := common.Marshal(ratios); marshalErr == nil {
			c.Header("X-New-Api-Other-Ratios", string(encoded))
		}
	}
	switch routing.Surface {
	case "native":
		if routing.RouteRender == "" || len(routing.RouteRequest) == 0 {
			presentFrozenTaskUnavailable(c)
			return
		}
		var request pluginruntime.RouteRequestContext
		if common.Unmarshal(routing.RouteRequest, &request) != nil {
			presentFrozenTaskUnavailable(c)
			return
		}
		view, err := service.BuildDurableTaskPluginView(c.Request.Context(), task)
		if err != nil {
			presentFrozenTaskUnavailable(c)
			return
		}
		viewValue, err := taskPluginProtocolJSONValue(view)
		if err != nil {
			presentFrozenTaskUnavailable(c)
			return
		}
		body, err := plugin.Engine.CallPath(c.Request.Context(), "native", []string{routing.RouteRender}, request.JSValue(), viewValue)
		if err != nil {
			presentFrozenTaskUnavailable(c)
			return
		}
		c.JSON(http.StatusOK, body)
	case "protocol":
		switch routing.Protocol {
		case "openai_video":
			video, err := service.BuildDurableVideoTask(c.Request.Context(), task, intake)
			if err != nil {
				presentFrozenTaskUnavailable(c)
				return
			}
			c.JSON(http.StatusOK, video)
		case pluginruntime.ProtocolOpenAIImage:
			presentDurableImage(c, task, plugin, routing, outcome)
		default:
			presentFrozenTaskUnavailable(c)
		}
	case "task":
		view, err := service.BuildDurableTaskPluginView(c.Request.Context(), task)
		if err != nil {
			presentFrozenTaskUnavailable(c)
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"id": task.TaskID, "task_id": task.TaskID,
			"status": task.Status, "model": task.Properties.OriginModelName,
			"created_at": max(task.CreatedAt, task.SubmitTime),
			"lifecycle":  view.Lifecycle,
		})
	default:
		presentFrozenTaskUnavailable(c)
	}
}

func presentFrozenTaskUnavailable(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{
		"message": "accepted task is temporarily unavailable", "type": "server_error", "code": "task_observation_unavailable",
	}})
}

func presentDurableImage(c *gin.Context, task *model.Task, plugin *pluginruntime.LoadedPlugin, routing service.DurableTaskRoutingSnapshot, outcome *model.PendingTaskOutcome) {
	var request pluginruntime.ProtocolRequestContext
	if common.Unmarshal(routing.ProtocolRequest, &request) != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	if task.Status != model.TaskStatusSuccess && task.Status != model.TaskStatusFailure {
		if err := waitDurableImageTask(c, task); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && c.Request.Context().Err() == nil {
				respondPluginProtocolError(c, http.StatusGatewayTimeout, "task_timeout", "image task "+task.TaskID+" is still running")
			} else if c.Request.Context().Err() == nil {
				presentFrozenTaskUnavailable(c)
			}
			return
		}
		if !routing.RetainResult {
			var err error
			task, outcome, err = service.DurableTaskDelivery(c.Request.Context(), task, false)
			if err != nil {
				presentFrozenTaskUnavailable(c)
				return
			}
		}
	}
	if task.Status == model.TaskStatusFailure {
		code := "task_failed"
		if outcome != nil && outcome.ErrorCode != "" {
			code = outcome.ErrorCode
		}
		status := http.StatusBadGateway
		message := "Task failed"
		switch code {
		case "invalid_input":
			message, status = "Invalid task input", http.StatusBadRequest
		case "unsupported_input":
			message, status = "Unsupported task input", http.StatusBadRequest
		case "content_policy_violation":
			message, status = "Task content was rejected", http.StatusBadRequest
		case "insufficient_user_quota":
			message, status = "Insufficient quota for task preparation", http.StatusTooManyRequests
		}
		if status != http.StatusBadGateway {
			c.JSON(status, gin.H{"message_key": message, "error": gin.H{
				"message": common.MessageWithRequestId(message, c.GetString(common.RequestIdKey)),
				"type":    "invalid_request_error",
				"code":    code,
			}})
		} else {
			respondPluginProtocolError(c, status, code, message)
		}
		return
	}
	view, err := service.BuildTaskPluginView(task)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	viewValue, err := taskPluginProtocolJSONValue(view)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	payload, err := plugin.Engine.CallPathWithAdmissionTimeout(c.Request.Context(), pluginruntime.DefaultCallTimeout,
		"protocols", []string{request.Protocol, "render"}, request.JSValue(), viewValue)
	if err != nil {
		presentFrozenTaskUnavailable(c)
		return
	}
	response, ok := payload.(map[string]any)
	data, hasData := response["data"].([]any)
	if !ok || !hasData {
		presentFrozenTaskUnavailable(c)
		return
	}
	if _, present := response["created"]; !present {
		response["created"] = max(task.CreatedAt, task.SubmitTime)
	}
	if taskPluginImageResponseFormat(request) == "b64_json" {
		for _, entry := range data {
			item, isObject := entry.(map[string]any)
			if !isObject {
				continue
			}
			url, _ := item["url"].(string)
			if existing, _ := item["b64_json"].(string); strings.TrimSpace(url) == "" || existing != "" {
				continue
			}
			_, encoded, downloadErr := service.GetImageFromUrl(url)
			if downloadErr == nil {
				item["b64_json"] = encoded
			}
		}
	}
	c.JSON(http.StatusOK, response)
}

// Images is synchronous to the client, while only the native worker advances
// the accepted task. This loop only reloads the existing row from primary DB.
func waitDurableImageTask(c *gin.Context, task *model.Task) error {
	timeout := time.Duration(constant.TaskPluginProtocolTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(taskPluginImagePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			current, exists, err := model.GetByTaskId(task.UserId, task.TaskID)
			if err != nil {
				return err
			}
			if !exists {
				return errors.New("accepted task disappeared")
			}
			*task = *current
			if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure {
				return nil
			}
		}
	}
}

func frozenTaskAdaptor(task *model.Task) (*jspluginadaptor.TaskAdaptor, error) {
	intake, plugin, err := service.LoadFrozenTaskPlugin(context.Background(), task.ID)
	if err != nil {
		return nil, err
	}
	var credentials service.DurableTaskCredentialSnapshot
	if err := common.Unmarshal(intake.CredentialSnapshot, &credentials); err != nil {
		return nil, err
	}
	channel := credentials.Channel
	if channel.ChannelId != task.ChannelId || channel.ChannelBaseUrl != intake.SupplierBaseURL || channel.ApiKey == "" {
		return nil, errors.New("frozen task credential identity is invalid")
	}
	adaptor := jspluginadaptor.New(plugin)
	adaptor.Init(&relaycommon.RelayInfo{ChannelMeta: &channel})
	return adaptor, nil
}
