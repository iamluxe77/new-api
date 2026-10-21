package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	pluginruntime "github.com/QuantumNous/new-api/pkg/jsplugin"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

func durableTaskPlugin(c *gin.Context) (*pluginruntime.LoadedPlugin, uint64) {
	value, _ := c.Get(pluginruntime.ContextKeyPinnedPlugin)
	pinned, _ := value.(pluginruntime.PinnedPlugin)
	if pinned.Plugin == nil || !pinned.Plugin.Meta.DurableSubmit() {
		return nil, 0
	}
	if pinned.Generation == nil {
		return pinned.Plugin, 0
	}
	return pinned.Plugin, pinned.Generation.Number
}

func executeDurableTaskSubmission(c *gin.Context, info *relaycommon.RelayInfo) (*taskSubmissionOutcome, *dto.TaskError) {
	plugin, generation := durableTaskPlugin(c)
	if plugin == nil {
		return nil, service.TaskErrorWrapperLocal(errors.New("durable plugin is unavailable"), "task_intake_invalid", http.StatusBadRequest)
	}
	if os.Getenv("TASK_DURABLE_SUBMIT_ENABLED") != "true" || common.BatchUpdateEnabled ||
		model.LOG_DB == nil || common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		return nil, service.TaskErrorWrapperLocal(errors.New("native task intake is not enabled"), "task_intake_unavailable", http.StatusServiceUnavailable)
	}
	if endpointValue, ok := c.Get(pluginruntime.ContextKeyPinnedEndpoint); ok {
		endpoint, _ := endpointValue.(pluginruntime.PinnedEndpoint)
		if endpoint.Protocol == "openai_responses" {
			return nil, service.TaskErrorWrapperLocal(errors.New("durable Responses submission is not supported"), "task_protocol_unsupported", http.StatusBadRequest)
		}
	}
	original, ok := middleware.OriginalTaskRequest(c)
	if !ok {
		return nil, service.TaskErrorWrapperLocal(errors.New("original task request was not captured"), "task_intake_invalid", http.StatusInternalServerError)
	}
	if key, other := c.GetHeader("Idempotency-Key"), c.GetHeader("X-Idempotency-Key"); key != "" && other != "" && key != other {
		return nil, service.TaskErrorWrapperLocal(errors.New("conflicting idempotency keys"), "task_intake_invalid", http.StatusBadRequest)
	}
	row, err := model.GetTaskPluginVersion(plugin.Meta.Key, plugin.Meta.Version)
	if err != nil || row.Id <= 0 || row.SourceHash != plugin.SourceHash || row.APIVersion != plugin.Meta.APIVersion {
		return nil, service.TaskErrorWrapperLocal(errors.New("executing plugin has no matching database source"), "task_plugin_source_unavailable", http.StatusServiceUnavailable)
	}
	admission, taskErr := relay.PrepareTaskAdmission(c, info)
	if taskErr != nil {
		return nil, taskErr
	}
	if admission == nil || admission.Platform != constant.TaskPlatform(plugin.Meta.Key) {
		return nil, service.TaskErrorWrapperLocal(errors.New("durable task admission returned an invalid plugin"), "task_intake_invalid", http.StatusInternalServerError)
	}
	inputValue, _ := c.Get("task_request")
	if inputValue == nil {
		inputValue = map[string]any{}
	}
	if len(info.OriginTasks) > 16 {
		return nil, service.TaskErrorWrapperLocal(errors.New("too many frozen origin tasks"), "task_intake_invalid", http.StatusBadRequest)
	}
	var originBytes int
	for _, origin := range info.OriginTasks {
		originBytes += len(origin.Data)
		if origin.TaskID == "" || len(origin.TaskID) > 128 || len(origin.UpstreamTaskID) > 191 ||
			len(origin.Data) > 64<<10 || originBytes > 512<<10 {
			return nil, service.TaskErrorWrapperLocal(errors.New("frozen origin task is invalid or too large"), "task_intake_invalid", http.StatusBadRequest)
		}
	}
	normalized, err := common.Marshal(inputValue)
	if err != nil || len(normalized) > model.MaxTaskRawInputBytes*2 {
		return nil, service.TaskErrorWrapperLocal(errors.New("normalized task input is too large"), "task_intake_invalid", http.StatusBadRequest)
	}
	var webhookIntent []byte
	if value, present := c.Get(pluginruntime.ContextKeyWebhookIntent); present {
		webhookIntent, err = common.Marshal(value)
		if err != nil || model.ValidateTaskWebhookIntent(webhookIntent) != nil {
			return nil, service.TaskErrorWrapperLocal(errors.New("invalid task webhook intent"), "task_webhook_invalid", http.StatusBadRequest)
		}
	}
	normalizedDigest := sha256.Sum256(normalized)
	webhookDigest := sha256.Sum256(webhookIntent)
	marker, err := common.Marshal(service.DurableTaskNormalizedMarker{
		Kind: "frozen-decoded-v1", SHA256: hex.EncodeToString(normalizedDigest[:]),
		Model: info.OriginModelName, Action: info.Action, WebhookSHA256: hex.EncodeToString(webhookDigest[:]),
	})
	if err != nil {
		return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusInternalServerError)
	}
	routing := service.DurableTaskRoutingSnapshot{
		Surface: "task", InputEncoding: "frozen-decoded-v1", BodyModelRewrite: middleware.FrozenTaskModelRewrite(c),
		RetainResult: true, ChannelType: info.ChannelType,
		ChannelID: info.ChannelId, OriginModel: info.OriginModelName,
		UpstreamModel: info.UpstreamModelName, Action: info.Action,
		OriginTasks: append([]relaycommon.OriginTaskRef(nil), info.OriginTasks...),
	}
	if routeValue, exists := c.Get(pluginruntime.ContextKeyPinnedRoute); exists {
		if route, valid := routeValue.(pluginruntime.PinnedRoute); valid {
			routing.Surface, routing.RouteRender, routing.RouteDecode = "native", route.Route.Render, route.Route.Decode
			if route.Route.RetainResult != nil {
				routing.RetainResult = *route.Route.RetainResult
			}
			requestValue, _ := c.Get(pluginruntime.ContextKeyRouteRequest)
			request, valid := requestValue.(pluginruntime.RouteRequestContext)
			if !valid {
				return nil, service.TaskErrorWrapperLocal(errors.New("route request is missing"), "task_intake_invalid", http.StatusInternalServerError)
			}
			request.Body = nil
			routing.RouteRequest, err = common.Marshal(request)
			if err != nil {
				return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusBadRequest)
			}
		}
	}
	if endpointValue, exists := c.Get(pluginruntime.ContextKeyPinnedEndpoint); exists {
		if endpoint, valid := endpointValue.(pluginruntime.PinnedEndpoint); valid {
			routing.Surface, routing.Protocol, routing.Operation = "protocol", endpoint.Protocol, endpoint.Operation.Name
			if endpoint.Protocol == pluginruntime.ProtocolOpenAIImage {
				routing.RetainResult = false
			}
			requestValue, _ := c.Get(pluginruntime.ContextKeyProtocolRequest)
			request, valid := requestValue.(pluginruntime.ProtocolRequestContext)
			if !valid {
				return nil, service.TaskErrorWrapperLocal(errors.New("protocol request is missing"), "task_intake_invalid", http.StatusInternalServerError)
			}
			request.Body = nil
			routing.ProtocolRequest, err = common.Marshal(request)
			if err != nil {
				return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusBadRequest)
			}
		}
	}
	selected := info.ChannelMeta
	if selected == nil || selected.ApiKey == "" || selected.ChannelBaseUrl == "" {
		return nil, service.TaskErrorWrapperLocal(errors.New("selected channel identity is incomplete"), "task_intake_invalid", http.StatusServiceUnavailable)
	}
	credential, err := common.Marshal(service.DurableTaskCredentialSnapshot{Channel: *selected})
	if err != nil {
		return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusInternalServerError)
	}
	routingJSON, err := common.Marshal(routing)
	if err != nil {
		return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusInternalServerError)
	}
	billingContext := model.TaskBillingContext{
		ModelPrice: info.PriceData.ModelPrice, GroupRatio: info.PriceData.GroupRatioInfo.GroupRatio,
		ModelRatio: info.PriceData.ModelRatio, OtherRatios: info.PriceData.OtherRatios(),
		OriginModelName: info.OriginModelName,
		PerCallBilling:  info.PriceData.UsePrice,
		TieredSnapshot:  info.TieredBillingSnapshot,
	}
	billingJSON, err := common.Marshal(service.DurableTaskBillingSnapshot{
		Context: billingContext, Quota: admission.Quota, QuotaPerUnit: common.QuotaPerUnit, Group: info.UsingGroup,
		TokenID: info.TokenId, ModelName: info.OriginModelName,
	})
	if err != nil {
		return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusInternalServerError)
	}
	accountDigest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", info.ChannelId, selected.ApiKey)))
	task := model.InitTask(admission.Platform, info)
	task.PrivateData = model.TaskPrivateData{}
	task.Action = info.Action
	queryRoutes, err := durableTaskQueryRoutes(plugin, task.TaskID)
	if err != nil {
		return nil, service.TaskErrorWrapperLocal(err, "task_intake_invalid", http.StatusBadRequest)
	}
	claim, err := model.ClaimTaskIntake(c.Request.Context(), task, model.TaskIntakeInput{
		ClientKey: original.ClientKey, Method: original.Method, Path: original.Path,
		RawQuery: original.RawQuery, ContentType: original.ContentType,
		RawInput: original.RawInput, SemanticHeaders: original.SemanticHeaders,
		NormalizedInput: marker, AccountID: hex.EncodeToString(accountDigest[:]),
		SupplierBaseURL: selected.ChannelBaseUrl, CredentialSnapshot: credential,
		RoutingSnapshot: routingJSON, ModelID: info.OriginModelName,
		PluginKey: plugin.Meta.Key, PluginVersion: plugin.Meta.Version,
		PluginRowID: row.Id, PluginSourceHash: row.SourceHash,
		PluginGeneration: generation, BillingSnapshot: billingJSON,
		WebhookIntent: webhookIntent,
		RetryPolicy:   model.CurrentRequestPolicy().DurableRetry,
		QueryRoutes:   queryRoutes,
	}, model.TaskReservation{TokenID: info.TokenId, Quota: admission.Quota})
	if err != nil {
		status, code := http.StatusServiceUnavailable, "task_intake_unavailable"
		if errors.Is(err, model.ErrTaskIntakeConflict) {
			status, code = http.StatusConflict, "idempotency_key_conflict"
		} else if errors.Is(err, model.ErrTaskQuotaInsufficient) {
			status, code = http.StatusForbidden, "insufficient_user_quota"
		} else if errors.Is(err, model.ErrTaskIntakeInvalid) || errors.Is(err, model.ErrTaskIntakeTooLarge) {
			status, code = http.StatusBadRequest, "task_intake_invalid"
			if errors.Is(err, model.ErrTaskIntakeTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
		}
		return nil, service.TaskErrorWrapperLocal(errors.New(code), code, status)
	}
	return &taskSubmissionOutcome{Task: claim.Task, RelayInfo: info, Result: admission}, nil
}

func durableTaskQueryRoutes(plugin *pluginruntime.LoadedPlugin, taskID string) ([]model.TaskIntakeQueryRouteInput, error) {
	if strings.ContainsAny(taskID, "/?#%") {
		return nil, model.ErrTaskIntakeInvalid
	}
	routes := make([]model.TaskIntakeQueryRouteInput, 0)
	for _, route := range plugin.Meta.Routes {
		if route.Type == pluginruntime.RouteTypeDynamic {
			return nil, errors.New("durable-submit@1 does not support native dynamic routes")
		}
		if route.Type != pluginruntime.RouteTypeQuery {
			continue
		}
		if route.Method != http.MethodGet {
			return nil, model.ErrTaskIntakeInvalid
		}
		parts := strings.Split(route.Path, "/")
		params := 0
		for i, part := range parts {
			if strings.HasPrefix(part, ":") || strings.HasPrefix(part, "*") {
				if part != ":"+route.TaskIDParam {
					return nil, model.ErrTaskIntakeInvalid
				}
				parts[i] = taskID
				params++
			}
		}
		if params != 1 {
			return nil, model.ErrTaskIntakeInvalid
		}
		routes = append(routes, model.TaskIntakeQueryRouteInput{
			Path: strings.Join(parts, "/"), Render: route.Render, TaskIDParam: route.TaskIDParam,
		})
	}
	return routes, nil
}
