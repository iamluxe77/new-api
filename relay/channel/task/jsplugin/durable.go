package jsplugin

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/sjson"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	pluginruntime "github.com/QuantumNous/new-api/pkg/jsplugin"
	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

type durablePrepareResult struct {
	State      any                      `json:"state"`
	UsageFacts map[string]any           `json:"usageFacts"`
	Rejection  *durablePrepareRejection `json:"rejection"`
}

type durableAdoptionProof struct {
	PublicTaskID     string `json:"public_task_id"`
	AcceptedAt       int64  `json:"accepted_at"`
	UserID           int    `json:"user_id"`
	ChannelID        int    `json:"channel_id"`
	AccountID        string `json:"account_id"`
	RawSHA256        string `json:"raw_sha256"`
	NormalizedSHA256 string `json:"normalized_sha256"`
	CommandSHA256    string `json:"command_sha256"`
}

func signDurableAdoptionProof(task *model.Task, intake *model.TaskIntake, command []byte) (string, error) {
	key, err := hex.DecodeString(os.Getenv("TASK_DURABLE_ADOPTION_PROOF_KEY"))
	if err != nil || len(key) != 32 || task == nil || intake == nil || task.SubmitTime <= 0 ||
		task.UserId != intake.UserID || task.ChannelId != intake.ChannelID {
		return "", errors.New("durable adoption proof configuration or intake is invalid")
	}
	raw := sha256.Sum256(intake.RawInput)
	normalized := sha256.Sum256(intake.NormalizedInput)
	commandHash := sha256.Sum256(command)
	payload, err := common.Marshal(durableAdoptionProof{PublicTaskID: task.TaskID, AcceptedAt: task.SubmitTime,
		UserID: task.UserId, ChannelID: intake.ChannelID, AccountID: intake.AccountID,
		RawSHA256: hex.EncodeToString(raw[:]), NormalizedSHA256: hex.EncodeToString(normalized[:]),
		CommandSHA256: hex.EncodeToString(commandHash[:])})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("new-api-durable-adoption-v1\x00"))
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + hex.EncodeToString(mac.Sum(nil)), nil
}

type durablePrepareRejection struct {
	Kind   string `json:"kind"`
	Code   string `json:"code"`
	Param  string `json:"param"`
	Width  int64  `json:"width"`
	Height int64  `json:"height"`
}

var durableRejectionCode = regexp.MustCompile(`^[a-z][a-z0-9_]{2,63}$`)

func validatedDurablePrepareRejection(rejection *durablePrepareRejection) (*service.DurableTaskPreparationRejection, error) {
	if rejection == nil {
		return nil, nil
	}
	if !durableRejectionCode.MatchString(rejection.Code) ||
		(rejection.Kind != "client_input" && rejection.Kind != "content") ||
		len(rejection.Param) > 128 || strings.ContainsAny(rejection.Param, "\x00\r\n") ||
		rejection.Width < 0 || rejection.Height < 0 || rejection.Width > 36000000 || rejection.Height > 36000000 {
		return nil, errors.New("invalid durable preparation rejection")
	}
	reason := "Invalid task input"
	if rejection.Kind == "content" {
		reason = "Task content was rejected"
	}
	return &service.DurableTaskPreparationRejection{Code: rejection.Code, Reason: reason}, nil
}

type durableReceiptFacts struct {
	Kind                          string `json:"kind"`
	ReceiptID                     string `json:"receiptId"`
	UpstreamTaskID                string `json:"upstreamTaskId"`
	SupplierNetQuota              int64  `json:"supplierNetQuota"`
	SupplierBillingFinal          bool   `json:"supplierBillingFinal"`
	NewSubmissionSafe             bool   `json:"newSubmissionSafe"`
	TemporaryTechnicalCause       bool   `json:"temporaryTechnicalCause"`
	PublicErrorCode               string `json:"publicErrorCode"`
	TaskLookupNotFound            bool   `json:"taskLookupNotFound"`
	FinalCustomerBillingReceiptID string `json:"finalCustomerBillingReceiptId"`
	WatchUpstreamTaskID           string `json:"watchUpstreamTaskId"`
	TaskData                      any    `json:"taskData"`
	State                         any    `json:"state"`
}

func (a *TaskAdaptor) durableSubmissionContext(ctx context.Context, task *model.Task, intake *model.TaskIntake) (*gin.Context, *relaycommon.RelayInfo, error) {
	if task == nil || intake == nil || a.info == nil || !a.info.HasChannelMeta() ||
		a.info.ChannelId != intake.ChannelID || a.info.ChannelBaseUrl != intake.SupplierBaseURL {
		return nil, nil, errors.New("frozen durable channel mismatch")
	}
	var routing service.DurableTaskRoutingSnapshot
	if err := common.Unmarshal(intake.RoutingSnapshot, &routing); err != nil {
		return nil, nil, fmt.Errorf("%w: routing snapshot: %v", service.ErrDurableFrozenInputMismatch, err)
	}
	var requestBody any
	if err := common.Unmarshal(intake.NormalizedInput, &requestBody); err != nil {
		return nil, nil, fmt.Errorf("%w: normalized input: %v", service.ErrDurableFrozenInputMismatch, err)
	}
	method := intake.Method
	if method == "" {
		method = http.MethodPost
	}
	requestURL := "http://frozen.invalid" + intake.Path
	if intake.RawQuery != "" {
		requestURL += "?" + intake.RawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(intake.RawInput))
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, fmt.Errorf("%w: request URL: %v", service.ErrDurableFrozenInputMismatch, err)
	}
	req.Header.Set("Content-Type", intake.ContentType)
	var semanticHeaders map[string]string
	if err := common.Unmarshal(intake.SemanticHeaders, &semanticHeaders); err != nil {
		return nil, nil, fmt.Errorf("%w: semantic headers: %v", service.ErrDurableFrozenInputMismatch, err)
	}
	if semanticHeaders["accept"] != "" {
		req.Header.Set("Accept", semanticHeaders["accept"])
	}
	c := &gin.Context{Request: req}
	requestBodyText := []byte(intake.RawInput)
	if routing.InputEncoding != "" && routing.InputEncoding != "frozen-decoded-v1" {
		return nil, nil, fmt.Errorf("%w: frozen input encoding is invalid", service.ErrDurableFrozenInputMismatch)
	}
	if routing.InputEncoding == "frozen-decoded-v1" {
		var marker service.DurableTaskNormalizedMarker
		if err := common.Unmarshal(intake.NormalizedInput, &marker); err != nil || marker.Kind != "frozen-decoded-v1" ||
			len(marker.SHA256) != 64 || len(marker.WebhookSHA256) != 64 ||
			marker.Model != intake.ModelID || marker.Model != routing.OriginModel || marker.Action != routing.Action {
			return nil, nil, fmt.Errorf("%w: frozen decoded input marker is invalid", service.ErrDurableFrozenInputMismatch)
		}
		decodeRaw := []byte(intake.RawInput)
		if routing.BodyModelRewrite != "" {
			if len(routing.BodyModelRewrite) > 191 {
				return nil, nil, fmt.Errorf("%w: frozen body rewrite is invalid", service.ErrDurableFrozenInputMismatch)
			}
			decodeRaw, err = sjson.SetBytes(decodeRaw, "model", routing.BodyModelRewrite)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: frozen body rewrite: %v", service.ErrDurableFrozenInputMismatch, err)
			}
		}
		requestBodyText = decodeRaw
		body, files, frozenFiles, err := pluginruntime.ParseFrozenTaskBody(decodeRaw, intake.ContentType)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: frozen body parse: %v", service.ErrDurableFrozenInputMismatch, err)
		}
		c.Set(pluginruntime.ContextKeyFrozenTaskFiles, frozenFiles)
		var decoded map[string]any
		switch routing.Surface {
		case "native":
			if routing.RouteDecode == "" || len(routing.RouteRequest) == 0 {
				return nil, nil, fmt.Errorf("%w: frozen route decoder is missing", service.ErrDurableFrozenInputMismatch)
			}
			var route pluginruntime.RouteRequestContext
			if err := common.Unmarshal(routing.RouteRequest, &route); err != nil || route.Path != intake.Path || route.Method != intake.Method || route.Body != nil {
				return nil, nil, fmt.Errorf("%w: frozen route identity is invalid", service.ErrDurableFrozenInputMismatch)
			}
			route.Body, route.Files = body, files
			if a.plugin.Meta.PreservesJSONOrder() && body["kind"] == string(pluginruntime.BodyJSON) {
				route.BodyText = intake.RawInput
			}
			value, _, callErr := a.plugin.Engine.CallPathWithMemberJSON(ctx, 0, a.plugin.Meta.JSONTextMember("requestBody"),
				"native", []string{routing.RouteDecode}, route.JSValueFor(a.plugin.Meta))
			decoded, _ = value.(map[string]any)
			if callErr != nil {
				if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) || ctx.Err() != nil {
					return nil, nil, callErr
				}
				return nil, nil, fmt.Errorf("%w: frozen route decoder: %v", service.ErrDurableFrozenInputMismatch, callErr)
			}
			c.Set(pluginruntime.ContextKeyRouteRequest, route)
		case "protocol":
			if len(routing.ProtocolRequest) == 0 || routing.Protocol == "" || routing.Operation == "" {
				return nil, nil, fmt.Errorf("%w: frozen protocol decoder is missing", service.ErrDurableFrozenInputMismatch)
			}
			var protocol pluginruntime.ProtocolRequestContext
			if err := common.Unmarshal(routing.ProtocolRequest, &protocol); err != nil ||
				protocol.Path != intake.Path || protocol.Method != intake.Method || protocol.Body != nil ||
				protocol.Protocol != routing.Protocol || protocol.Operation != routing.Operation {
				return nil, nil, fmt.Errorf("%w: frozen protocol identity is invalid", service.ErrDurableFrozenInputMismatch)
			}
			protocol.Body, protocol.Files = body, files
			if a.plugin.Meta.PreservesJSONOrder() && body["kind"] == string(pluginruntime.BodyJSON) {
				protocol.BodyText = decodeRaw
			}
			value, _, callErr := a.plugin.Engine.CallPathWithMemberJSON(ctx, 0, a.plugin.Meta.JSONTextMember("requestBody"),
				"protocols", []string{routing.Protocol, "decodeRequest"}, protocol.JSValueFor(a.plugin.Meta))
			decoded, _ = value.(map[string]any)
			if callErr != nil {
				if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) || ctx.Err() != nil {
					return nil, nil, callErr
				}
				return nil, nil, fmt.Errorf("%w: frozen protocol decoder: %v", service.ErrDurableFrozenInputMismatch, callErr)
			}
			c.Set(pluginruntime.ContextKeyProtocolRequest, protocol)
			c.Set(pluginruntime.ContextKeyRouteRequest, protocol.RouteRequestContext)
		case "task":
			if body["kind"] != string(pluginruntime.BodyJSON) {
				return nil, nil, fmt.Errorf("%w: frozen task body is not JSON", service.ErrDurableFrozenInputMismatch)
			}
			value, valid := body["value"].(map[string]any)
			if !valid {
				return nil, nil, fmt.Errorf("%w: frozen task body is invalid", service.ErrDurableFrozenInputMismatch)
			}
			decoded = map[string]any{"kind": "submit", "model": marker.Model, "requestBody": value}
		default:
			return nil, nil, fmt.Errorf("%w: frozen task surface is invalid", service.ErrDurableFrozenInputMismatch)
		}
		if decoded == nil || decoded["kind"] != "submit" || decoded["model"] != marker.Model {
			return nil, nil, fmt.Errorf("%w: frozen decoder changed task identity", service.ErrDurableFrozenInputMismatch)
		}
		if action, present := decoded["action"]; present && action != marker.Action {
			return nil, nil, fmt.Errorf("%w: frozen decoder changed task action", service.ErrDurableFrozenInputMismatch)
		}
		requestBody = decoded["requestBody"]
		normalized, err := common.Marshal(requestBody)
		if err != nil || len(normalized) > model.MaxTaskRawInputBytes*2 {
			return nil, nil, fmt.Errorf("%w: frozen decoded input is too large", service.ErrDurableFrozenInputMismatch)
		}
		digest := sha256.Sum256(normalized)
		if hex.EncodeToString(digest[:]) != marker.SHA256 {
			return nil, nil, fmt.Errorf("%w: decoded digest changed", service.ErrDurableFrozenInputMismatch)
		}
		var webhook []byte
		if value, present := decoded["webhook"]; present {
			webhook, err = common.Marshal(value)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: frozen webhook intent: %v", service.ErrDurableFrozenInputMismatch, err)
			}
		}
		webhookDigest := sha256.Sum256(webhook)
		if hex.EncodeToString(webhookDigest[:]) != marker.WebhookSHA256 || !bytes.Equal(webhook, intake.WebhookIntent) {
			return nil, nil, fmt.Errorf("%w: webhook intent changed", service.ErrDurableFrozenInputMismatch)
		}
	}
	c.Set("task_request", requestBody)
	if len(routing.RouteRequest) > 0 {
		var route pluginruntime.RouteRequestContext
		if err := common.Unmarshal(routing.RouteRequest, &route); err != nil {
			return nil, nil, fmt.Errorf("%w: frozen route request: %v", service.ErrDurableFrozenInputMismatch, err)
		}
		if _, frozen := c.Get(pluginruntime.ContextKeyFrozenTaskFiles); !frozen {
			c.Set(pluginruntime.ContextKeyRouteRequest, route)
		}
	}
	if a.plugin.Meta.PreservesJSONOrder() && strings.HasPrefix(intake.ContentType, "application/json") {
		c.Set(pluginruntime.ContextKeyRequestBodyText, json.RawMessage(requestBodyText))
	}
	info := &relaycommon.RelayInfo{ChannelMeta: a.info.ChannelMeta, PublicTaskID: task.TaskID,
		Action: routing.Action, OriginModelName: routing.OriginModel,
		UpstreamModelName: routing.UpstreamModel, UserId: task.UserId}
	if len(routing.OriginTasks) > 16 {
		return nil, nil, fmt.Errorf("%w: too many frozen origin tasks", service.ErrDurableFrozenInputMismatch)
	}
	info.TaskRelayInfo = &relaycommon.TaskRelayInfo{OriginTasks: routing.OriginTasks}
	var originBytes int
	for _, origin := range routing.OriginTasks {
		originBytes += len(origin.Data)
		if origin.TaskID == "" || len(origin.TaskID) > 128 || len(origin.UpstreamTaskID) > 191 ||
			len(origin.Data) > 64<<10 || originBytes > 512<<10 {
			return nil, nil, fmt.Errorf("%w: frozen origin task is invalid or too large", service.ErrDurableFrozenInputMismatch)
		}
	}
	a.durableWorker = true
	return c, info, nil
}

func (a *TaskAdaptor) PrepareDurableTask(ctx context.Context, task *model.Task, intake *model.TaskIntake) (service.DurableTaskPreparation, error) {
	var result service.DurableTaskPreparation
	if !a.plugin.Meta.DurableSubmit() {
		return result, errors.New("plugin is not durable")
	}
	c, info, err := a.durableSubmissionContext(ctx, task, intake)
	if err != nil {
		return result, err
	}
	if a.hasHook(ctx, "buildPrepareRequest") {
		prepareCtx := a.submitContext(c, info)
		descriptor, err := a.requestDescriptor(ctx, "buildPrepareRequest", prepareCtx)
		if err != nil {
			return result, err
		}
		if descriptor.Operation == "reject" {
			if descriptor.URL != "" || descriptor.Method != "" || len(descriptor.Headers) != 0 || descriptor.Body != nil ||
				len(descriptor.BodyText) != 0 || descriptor.SupplierKeyHeader != "" || descriptor.AtomicReplay != nil || descriptor.ResponseType != "" ||
				descriptor.BodyType != "" || len(descriptor.Parts) != 0 || descriptor.Credentialless ||
				descriptor.Action != "" || descriptor.Model != "" || descriptor.RewriteModel != "" {
				return result, errors.New("invalid local preparation rejection descriptor")
			}
			rejected, err := validatedDurablePrepareRejection(descriptor.Rejection)
			if err != nil || rejected == nil {
				return result, errors.New("invalid local preparation rejection")
			}
			return result, rejected
		}
		if descriptor.Rejection != nil {
			return result, errors.New("unexpected preparation rejection")
		}
		if descriptor.SupplierKeyHeader != "" || descriptor.ResponseType == "sse" {
			return result, errors.New("invalid preparation descriptor")
		}
		if descriptor.BodyType != "" && descriptor.BodyType != "json" || len(descriptor.Parts) != 0 {
			return result, errors.New("durable preparation requires a JSON body")
		}
		if descriptor.Body != nil {
			descriptor.Body, err = inlineJSONFilePlaceholders(c, descriptor.Body)
			if err != nil {
				return result, err
			}
			descriptor.BodyText = nil
		}
		if descriptor.Operation == "adopt" {
			if descriptor.Credentialless || descriptor.Method != http.MethodPost || descriptor.Body == nil ||
				len(descriptor.Headers) > 16 {
				return result, errors.New("invalid original adoption descriptor")
			}
			body, marshalErr := common.Marshal(descriptor.Body)
			if marshalErr != nil || len(body) > 1<<20 {
				return result, errors.New("original adoption command exceeds limit")
			}
			proof, proofErr := signDurableAdoptionProof(task, intake, body)
			if proofErr != nil {
				return result, proofErr
			}
			base, parseErr := url.Parse(intake.SupplierBaseURL)
			uri, uriErr := url.Parse(descriptor.URL)
			if parseErr != nil || uriErr != nil || uri.Scheme != base.Scheme || uri.Host != base.Host ||
				uri.Path == "" || uri.RawQuery != "" ||
				pluginruntime.ValidateRequestURL(descriptor.URL, intake.SupplierBaseURL, a.plugin.Meta.AllowedHosts) != nil {
				return result, errors.New("original adoption URL is invalid")
			}
			headers := map[string]string{"Content-Type": "application/json", "Accept": "application/json",
				"Authorization": "Bearer " + info.ApiKey, "X-New-Api-Public-Task": task.TaskID,
				"X-New-Api-Durable-Proof": proof}
			result.Wire = model.TaskPreparedWire{Operation: "adopt", BaseURL: intake.SupplierBaseURL,
				Host: base.Host, Method: http.MethodPost, Path: uri.RequestURI(), ContentType: "application/json",
				Headers: headers, SupplierKeyHeader: "Idempotency-Key", Body: body, AccountID: intake.AccountID}
			result.Operation = "adopt"
			return result, nil
		}
		if descriptor.Operation != "" {
			return result, errors.New("unsupported durable preparation operation")
		}
		observation, err := a.durableDescriptorHTTPWithReceipt(ctx, descriptor, http.MethodPost, intake.SupplierBaseURL,
			info.ApiKey, info.ChannelSetting.Proxy, 1<<20, 176<<20, "", nil)
		if err != nil {
			return result, err
		}
		response := durableHookResponse(observation)
		value, err := a.plugin.Engine.Call(ctx, "parsePrepareResponse", prepareCtx, response)
		if err != nil {
			return result, err
		}
		var prepared durablePrepareResult
		if err := convert(value, &prepared); err != nil {
			return result, err
		}
		if prepared.Rejection != nil {
			if prepared.State != nil || len(prepared.UsageFacts) != 0 {
				return result, errors.New("preparation rejection cannot include prepared state or usage")
			}
			rejected, err := validatedDurablePrepareRejection(prepared.Rejection)
			if err != nil {
				return result, err
			}
			return result, rejected
		}
		if prepared.State != nil {
			state, err := common.Marshal(prepared.State)
			if err != nil || len(state) > 1<<20 {
				return result, errors.New("invalid durable preparation state")
			}
		}
		a.prepareState = prepared.State
		result.UsageFacts = prepared.UsageFacts
		// Multipart input is parsed again from immutable raw bytes when the
		// final descriptor is built; preparation cannot consume its file refs.
		c, info, err = a.durableSubmissionContext(ctx, task, intake)
		if err != nil {
			return result, err
		}
	}
	descriptor, err := a.buildSubmit(c, info)
	if err != nil {
		return result, err
	}
	if descriptor.ResponseType != "json" || descriptor.SupplierKeyHeader == "" || descriptor.Credentialless ||
		descriptor.BodyType == "multipart" || len(descriptor.Parts) != 0 ||
		strings.EqualFold(descriptor.SupplierKeyHeader, "Host") {
		return result, errors.New("durable submit requires JSON and host supplier key header")
	}
	for name := range descriptor.Headers {
		if strings.EqualFold(name, descriptor.SupplierKeyHeader) {
			return result, errors.New("plugin supplied the host attempt key header")
		}
	}
	bodyReader, err := a.BuildRequestBody(c, info)
	if err != nil {
		return result, err
	}
	var body []byte
	if bodyReader != nil {
		body, err = io.ReadAll(io.LimitReader(bodyReader, model.MaxTaskRawInputBytes+1))
		if err != nil || len(body) > model.MaxTaskRawInputBytes {
			return result, errors.New("durable supplier body exceeds limit")
		}
	}
	method := strings.ToUpper(descriptor.Method)
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost {
		return result, errors.New("durable paid command must be POST")
	}
	uri, err := url.Parse(descriptor.URL)
	if err != nil {
		return result, err
	}
	base, err := url.Parse(intake.SupplierBaseURL)
	if err != nil || !strings.EqualFold(uri.Host, base.Host) || uri.Scheme != base.Scheme {
		return result, errors.New("durable paid command must use frozen account host")
	}
	req, err := http.NewRequestWithContext(ctx, method, descriptor.URL, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	if err := a.BuildRequestHeader(c, req, info); err != nil {
		return result, err
	}
	if err := channel.ApplyHeaderOverride(a.headerOverrideInfo(info.ApiKey), nil, req); err != nil {
		return result, err
	}
	if req.Header.Get(descriptor.SupplierKeyHeader) != "" {
		return result, errors.New("host attempt key header is already populated")
	}
	headers := make(map[string]string, len(req.Header))
	for name, values := range req.Header {
		if len(values) > 0 {
			headers[name] = values[0]
		}
	}
	result.Wire = model.TaskPreparedWire{BaseURL: intake.SupplierBaseURL, Host: req.Host, Method: method,
		Path: uri.RequestURI(), ContentType: req.Header.Get("Content-Type"),
		Headers: headers, SupplierKeyHeader: descriptor.SupplierKeyHeader,
		Body: body, AccountID: intake.AccountID, AtomicReplay: descriptor.AtomicReplay}
	if a.prepareState != nil {
		result.Wire.PreparedState, err = common.Marshal(a.prepareState)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (a *TaskAdaptor) durableDescriptorHTTP(ctx context.Context, descriptor requestDescriptor, defaultMethod, baseURL, key, proxy string, limit int64) (model.TaskHTTPObservation, error) {
	return a.durableDescriptorHTTPWithReceipt(ctx, descriptor, defaultMethod, baseURL, key, proxy, limit, limit, "", nil)
}

func (a *TaskAdaptor) durableDescriptorHTTPWithReceipt(ctx context.Context, descriptor requestDescriptor, defaultMethod, baseURL, key, proxy string,
	limit, requestLimit int64, supplierKey string, receiptWire *model.TaskReceiptEvidence, auditLimit ...int64) (model.TaskHTTPObservation, error) {
	var observation model.TaskHTTPObservation
	if err := pluginruntime.ValidateRequestURL(descriptor.URL, baseURL, a.plugin.Meta.AllowedHosts); err != nil {
		return observation, err
	}
	method := strings.ToUpper(descriptor.Method)
	if method == "" {
		method = defaultMethod
	}
	var body []byte
	if descriptor.Body != nil {
		if descriptor.BodyText != nil {
			body = descriptor.BodyText
		} else if text, ok := descriptor.Body.(string); ok {
			body = []byte(text)
		} else {
			var err error
			body, err = common.Marshal(descriptor.Body)
			if err != nil {
				return observation, err
			}
		}
	}
	if int64(len(body)) > requestLimit {
		return observation, errors.New("durable request body exceeds limit")
	}
	req, err := http.NewRequestWithContext(ctx, method, descriptor.URL, bytes.NewReader(body))
	if err != nil {
		return observation, err
	}
	if method == http.MethodPost && descriptor.SupplierKeyHeader != "" {
		// A paid command must not be replayed by net/http after a broken reused connection.
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.GetBody = nil
	}
	for name, value := range descriptor.Headers {
		req.Header.Set(name, value)
	}
	if descriptor.WireHost != "" {
		req.Host = descriptor.WireHost
	}
	if key != "" {
		if err := channel.ApplyHeaderOverride(a.headerOverrideInfo(key), nil, req); err != nil {
			return observation, err
		}
	}
	if receiptWire != nil {
		if method != http.MethodGet || supplierKey == "" {
			return observation, errors.New("receipt request binding is invalid")
		}
		if expectedAuth := receiptWire.RequestHeaders["Authorization"]; expectedAuth != "" &&
			(req.Header.Get("Authorization") != expectedAuth || req.Host != req.URL.Host ||
				(req.Header.Get("Host") != "" && req.Header.Get("Host") != req.Host)) {
			return observation, errors.New("frozen receipt authentication or Host changed")
		}
		effectiveHeaders := make(map[string]string, len(req.Header))
		headerBytes := 0
		for name, values := range req.Header {
			if len(values) > 0 {
				headerBytes += len(name) + len(values[0])
				if len(req.Header) > 32 || len(values[0]) > 4096 || headerBytes > 8<<10 {
					return observation, errors.New("receipt request headers exceed evidence limit")
				}
				effectiveHeaders[name] = values[0]
			}
		}
		if !durableReceiptURLBindsKey(req.URL, supplierKey, effectiveHeaders) {
			return observation, errors.New("effective receipt GET does not bind the frozen supplier key")
		}
		receiptWire.Method, receiptWire.URL, receiptWire.RequestHost, receiptWire.RequestHeaders =
			req.Method, req.URL.String(), req.Host, effectiveHeaders
	}
	client, err := service.GetHttpClientWithProxySettings(proxy, a.channelSetting())
	if err != nil {
		return observation, err
	}
	bounded := *client
	bounded.Timeout = 30 * time.Second
	bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := bounded.Do(req)
	if err != nil {
		if resp != nil {
			observation.StatusCode = resp.StatusCode
			observation.Headers = durableRetryHeaderEvidence(resp.Header)
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		return observation, err
	}
	defer resp.Body.Close()
	observation.StatusCode = resp.StatusCode
	observation.Headers = durableRetryHeaderEvidence(resp.Header)
	boundedAudit := len(auditLimit) > 0
	headers := make(http.Header)
	for name, values := range resp.Header {
		canonical := http.CanonicalHeaderKey(name)
		if len(headers) >= 32 || len(values) == 0 || len(values) > 4 {
			if !boundedAudit {
				return observation, errors.New("durable response headers exceed limit")
			}
			observation.HeadersTruncated = true
			continue
		}
		valid := true
		for _, value := range values {
			if len(value) > 4096 {
				valid = false
			}
		}
		if !valid {
			if !boundedAudit {
				return observation, errors.New("durable response header exceeds limit")
			}
			observation.HeadersTruncated = true
			continue
		}
		headers[canonical] = append([]string(nil), values...)
	}
	headerJSON, err := common.Marshal(headers)
	if err != nil {
		return observation, err
	}
	if len(headerJSON) > 16<<10 {
		if !boundedAudit {
			return observation, errors.New("durable response headers exceed limit")
		}
		headerJSON = []byte("{}")
		observation.HeadersTruncated = true
	}
	observation.Headers = model.TaskBlob(headerJSON)
	if boundedAudit {
		if auditLimit[0] <= 0 || auditLimit[0] > model.MaxTaskSubmitAuditBodyBytes {
			return observation, errors.New("durable submit audit limit is invalid")
		}
		prefix, readErr := io.ReadAll(io.LimitReader(resp.Body, auditLimit[0]))
		hash := sha256.New()
		_, _ = hash.Write(prefix)
		var rest int64
		if readErr == nil {
			rest, readErr = io.Copy(hash, resp.Body)
		}
		observation.Body = model.TaskBlob(prefix)
		observation.BodyLength = int64(len(prefix)) + rest
		observation.BodySHA256 = hex.EncodeToString(hash.Sum(nil))
		observation.BodyReadError = readErr != nil
		observation.BodyTruncated = rest > 0 || readErr != nil
		return observation, nil
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	observation.Body = model.TaskBlob(responseBody[:min(int64(len(responseBody)), limit)])
	if err != nil || int64(len(responseBody)) > limit {
		return observation, errors.New("durable response exceeds limit")
	}
	observation.Body = model.TaskBlob(responseBody)
	return observation, nil
}

func durableHookResponse(observation model.TaskHTTPObservation) map[string]any {
	var headers http.Header
	_ = common.Unmarshal(observation.Headers, &headers)
	firstValues := make(map[string]string, len(headers))
	for name, values := range headers {
		if len(values) > 0 {
			firstValues[name] = values[0]
		}
	}
	var body any = string(observation.Body)
	var decoded any
	if common.Unmarshal(observation.Body, &decoded) == nil {
		body = decoded
	}
	return map[string]any{"statusCode": observation.StatusCode, "headers": firstValues, "body": body}
}

func durableRetryHeaderEvidence(headers http.Header) model.TaskBlob {
	values := headers.Values("Retry-After")
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 64 ||
		strings.TrimSpace(values[0]) != values[0] {
		return model.TaskBlob("{}")
	}
	encoded, err := common.Marshal(http.Header{"Retry-After": []string{values[0]}})
	if err != nil {
		return model.TaskBlob("{}")
	}
	return model.TaskBlob(encoded)
}

func durableReceiptRetryNotBefore(observation model.TaskHTTPObservation) int64 {
	var headers http.Header
	if common.Unmarshal(observation.Headers, &headers) != nil {
		return 0
	}
	values := headers.Values("Retry-After")
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 64 ||
		strings.TrimSpace(values[0]) != values[0] {
		return 0
	}
	now := time.Now()
	if seconds, err := strconv.ParseInt(values[0], 10, 64); err == nil {
		if seconds < 1 || seconds > 3600 {
			return 0
		}
		return now.Add(time.Duration(seconds) * time.Second).UnixMilli()
	}
	deadline, err := http.ParseTime(values[0])
	if err != nil || !deadline.After(now) || deadline.After(now.Add(time.Hour)) {
		return 0
	}
	return deadline.UnixMilli()
}

func (a *TaskAdaptor) CheckDurableReceipt(ctx context.Context, task *model.Task, intake *model.TaskIntake, dispatch model.TaskDispatch) (service.DurableTaskReceipt, error) {
	var receipt service.DurableTaskReceipt
	if dispatch.SupplierKey == "" || dispatch.RequestHash == "" {
		return receipt, errors.New("frozen receipt identity is missing")
	}
	key, baseURL, proxy := a.queryCredentials()
	query, err := a.queryContext(task, key, baseURL, proxy)
	if err != nil {
		return receipt, err
	}
	bodyHash := sha256.Sum256(dispatch.WireBody)
	query["attempt"], query["supplierKey"], query["accountId"] = dispatch.Attempt, dispatch.SupplierKey, intake.AccountID
	query["requestHash"], query["requestBodyHash"] = dispatch.RequestHash, hex.EncodeToString(bodyHash[:])
	if len(dispatch.PreparedState) > 0 {
		var prepared any
		if err := common.Unmarshal(dispatch.PreparedState, &prepared); err != nil {
			return receipt, err
		}
		query["prepareState"] = prepared
	}
	descriptor, err := a.requestDescriptor(ctx, "buildReceiptRequest", query)
	if err != nil {
		return receipt, err
	}
	if descriptor.Method != http.MethodGet || descriptor.Body != nil || descriptor.SupplierKeyHeader != "" {
		return receipt, errors.New("receipt request must be GET")
	}
	parsedURL, err := url.Parse(descriptor.URL)
	base, baseErr := url.Parse(intake.SupplierBaseURL)
	if err != nil || baseErr != nil || parsedURL.Host != base.Host || parsedURL.Scheme != base.Scheme {
		return receipt, errors.New("receipt request crossed frozen account host")
	}
	var receiptWire model.TaskReceiptEvidence
	if dispatch.AtomicReplaySeconds > 0 {
		var wireHeaders map[string]string
		if err := common.Unmarshal(dispatch.WireHeaders, &wireHeaders); err != nil ||
			dispatch.AccountID != intake.AccountID || dispatch.WireBaseURL != intake.SupplierBaseURL ||
			!durableReceiptURLBindsKey(parsedURL, dispatch.SupplierKey, descriptor.Headers) ||
			wireHeaders["Authorization"] == "" || descriptor.Headers["Authorization"] != wireHeaders["Authorization"] ||
			key == "" || descriptor.Headers["Authorization"] != "Bearer "+key {
			return receipt, errors.New("frozen receipt request authentication or route changed")
		}
		receiptWire.RequestHeaders = map[string]string{"Authorization": wireHeaders["Authorization"]}
	}
	observation, err := a.durableDescriptorHTTPWithReceipt(ctx, descriptor, http.MethodGet, baseURL, key, proxy, 32<<10, 32<<10,
		dispatch.SupplierKey, &receiptWire)
	receipt.RetryNotBeforeAt = durableReceiptRetryNotBefore(observation)
	if observation.StatusCode == http.StatusOK {
		seen, seenErr := durableSeenReceipt(receiptWire, observation, intake, dispatch, bodyHash, err)
		seen.RetryNotBeforeAt = receipt.RetryNotBeforeAt
		return seen, seenErr
	}
	if err != nil {
		return receipt, err
	}
	if observation.StatusCode == http.StatusNotFound {
		receipt.Missing = true
		return receipt, nil
	}
	return receipt, fmt.Errorf("receipt HTTP status %d", observation.StatusCode)
}

func (a *TaskAdaptor) CheckDurableAttemptReceipt(ctx context.Context, task *model.Task, intake *model.TaskIntake,
	attempt model.TaskDispatchAttempt) (service.DurableTaskReceipt, error) {
	var dispatch model.TaskDispatch
	if task == nil || intake == nil || attempt.TaskID != task.ID || attempt.Ordinal < 1 ||
		attempt.AccountID != intake.AccountID || attempt.SupplierKey == "" || attempt.RequestHash == "" {
		return service.DurableTaskReceipt{}, errors.New("historical receipt attempt is invalid")
	}
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
		return service.DurableTaskReceipt{}, err
	}
	if dispatch.Attempt < attempt.Ordinal || dispatch.AccountID != attempt.AccountID ||
		dispatch.RequestHash != attempt.RequestHash || dispatch.WireBaseURL != intake.SupplierBaseURL ||
		dispatch.AtomicReplaySeconds <= 0 {
		return service.DurableTaskReceipt{}, errors.New("historical receipt changed frozen wire")
	}
	dispatch.Attempt, dispatch.SupplierKey = attempt.Ordinal, attempt.SupplierKey
	return a.CheckDurableReceipt(ctx, task, intake, dispatch)
}

func durableSeenReceipt(wire model.TaskReceiptEvidence, observation model.TaskHTTPObservation, intake *model.TaskIntake,
	dispatch model.TaskDispatch, bodyHash [sha256.Size]byte, responseErr error) (service.DurableTaskReceipt, error) {
	wire.AccountID, wire.SupplierKey, wire.RequestHash = intake.AccountID, dispatch.SupplierKey, dispatch.RequestHash
	wire.RequestBodyHash = hex.EncodeToString(bodyHash[:])
	wire.StatusCode, wire.Headers, wire.Body = observation.StatusCode, observation.Headers, observation.Body
	if len(wire.Headers) == 0 {
		wire.Headers = model.TaskBlob("{}")
	}
	wire.ResponseIncomplete = responseErr != nil
	wire.ObservedAt = time.Now().Unix()
	evidence, err := common.Marshal(wire)
	if err != nil {
		return service.DurableTaskReceipt{}, err
	}
	if len(evidence) > 64<<10 {
		wire.Body = wire.Body[:min(len(wire.Body), 8<<10)]
		wire.ResponseIncomplete = true
		evidence, err = common.Marshal(wire)
		if err != nil || len(evidence) > 64<<10 {
			return service.DurableTaskReceipt{}, errors.New("bounded receipt evidence is unavailable")
		}
	}
	return service.DurableTaskReceipt{Seen: true, Evidence: evidence}, nil
}

func durableReceiptURLBindsKey(parsedURL *url.URL, supplierKey string, headers map[string]string) bool {
	for _, segment := range strings.Split(parsedURL.EscapedPath(), "/") {
		decoded, decodeErr := url.PathUnescape(segment)
		if decodeErr == nil && decoded == supplierKey {
			return true
		}
	}
	for _, values := range parsedURL.Query() {
		for _, value := range values {
			if value == supplierKey {
				return true
			}
		}
	}
	for _, value := range headers {
		if value == supplierKey {
			return true
		}
	}
	return false
}

func (a *TaskAdaptor) VerifyDurableReceipt(ctx context.Context, task *model.Task, intake *model.TaskIntake,
	attempt model.TaskDispatchAttempt, receipt model.TaskReceipt) (model.TaskVerifiedReceipt, error) {
	var verified model.TaskVerifiedReceipt
	if len(receipt.Evidence) == 0 || len(receipt.Evidence) > 64<<10 || intake == nil || task == nil {
		return verified, errors.New("receipt evidence is invalid")
	}
	var evidence model.TaskReceiptEvidence
	if err := common.Unmarshal(receipt.Evidence, &evidence); err != nil {
		return verified, err
	}
	if evidence.Method != http.MethodGet || evidence.StatusCode != http.StatusOK || evidence.ObservedAt <= 0 ||
		evidence.ResponseIncomplete ||
		evidence.AccountID != attempt.AccountID || evidence.AccountID != intake.AccountID ||
		evidence.RequestHash != attempt.RequestHash || evidence.SupplierKey != attempt.SupplierKey ||
		len(evidence.Body) == 0 || len(evidence.Body) > 32<<10 || len(evidence.Headers) > 16<<10 {
		return verified, errors.New("receipt evidence does not bind the frozen attempt")
	}
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", attempt.TaskID).Take(&dispatch).Error; err != nil {
		return verified, err
	}
	bodyHash := sha256.Sum256(dispatch.WireBody)
	if dispatch.AccountID != attempt.AccountID || dispatch.RequestHash != attempt.RequestHash ||
		dispatch.Attempt < attempt.Ordinal || evidence.RequestBodyHash != hex.EncodeToString(bodyHash[:]) {
		return verified, errors.New("receipt evidence body binding changed")
	}
	parsedURL, err := url.Parse(evidence.URL)
	base, baseErr := url.Parse(intake.SupplierBaseURL)
	if err != nil || baseErr != nil || parsedURL.Host != base.Host || parsedURL.Scheme != base.Scheme ||
		!durableReceiptURLBindsKey(parsedURL, attempt.SupplierKey, evidence.RequestHeaders) {
		return verified, errors.New("receipt evidence URL is not account/key bound")
	}
	if dispatch.AtomicReplaySeconds > 0 {
		var wireHeaders map[string]string
		if err := common.Unmarshal(dispatch.WireHeaders, &wireHeaders); err != nil ||
			wireHeaders["Authorization"] == "" || evidence.RequestHeaders["Authorization"] != wireHeaders["Authorization"] {
			return verified, errors.New("receipt evidence is not authenticated to the frozen receipt route")
		}
	}
	if evidence.RequestHost == "" ||
		(evidence.RequestHeaders["Host"] != "" && evidence.RequestHost != evidence.RequestHeaders["Host"]) ||
		(evidence.RequestHeaders["Host"] == "" && evidence.RequestHost != parsedURL.Host) {
		return verified, errors.New("receipt evidence virtual host is inconsistent")
	}
	var headers http.Header
	if err := common.Unmarshal(evidence.Headers, &headers); err != nil || len(headers) > 32 {
		return verified, errors.New("receipt evidence headers are invalid")
	}
	key, baseURL, proxy := a.queryCredentials()
	query, err := a.queryContext(task, key, baseURL, proxy)
	if err != nil {
		return verified, err
	}
	query["attempt"], query["supplierKey"], query["accountId"] = attempt.Ordinal, attempt.SupplierKey, attempt.AccountID
	query["requestHash"], query["requestBodyHash"] = attempt.RequestHash, evidence.RequestBodyHash
	if len(dispatch.PreparedState) > 0 {
		var prepared any
		if err := common.Unmarshal(dispatch.PreparedState, &prepared); err != nil {
			return verified, err
		}
		query["prepareState"] = prepared
	}
	observation := model.TaskHTTPObservation{StatusCode: evidence.StatusCode, Headers: evidence.Headers, Body: evidence.Body}
	value, err := a.plugin.Engine.Call(ctx, "parseReceiptResponse", query, durableHookResponse(observation))
	if err != nil {
		return verified, err
	}
	var facts durableReceiptFacts
	if err := convert(value, &facts); err != nil {
		return verified, err
	}
	if facts.TaskLookupNotFound || facts.FinalCustomerBillingReceiptID != "" || facts.WatchUpstreamTaskID != "" {
		if len(facts.FinalCustomerBillingReceiptID) > 191 || len(facts.WatchUpstreamTaskID) > 191 ||
			facts.Kind == "unknown" && (facts.TaskLookupNotFound != (facts.WatchUpstreamTaskID != "") ||
				facts.TaskLookupNotFound && (facts.FinalCustomerBillingReceiptID == "" || facts.UpstreamTaskID != "" ||
					facts.NewSubmissionSafe || facts.SupplierBillingFinal || facts.SupplierNetQuota != 0)) ||
			facts.Kind != "unknown" && (facts.TaskLookupNotFound || facts.WatchUpstreamTaskID != "") {
			return verified, errors.New("uncertain receipt facts are invalid")
		}
	}
	digest := sha256.Sum256(receipt.Evidence)
	verified = model.TaskVerifiedReceipt{Kind: facts.Kind, ReceiptID: facts.ReceiptID,
		EvidenceHash: hex.EncodeToString(digest[:]), AccountID: attempt.AccountID,
		RequestHash: attempt.RequestHash, SupplierKey: attempt.SupplierKey,
		UpstreamID: facts.UpstreamTaskID, SupplierNetQuota: facts.SupplierNetQuota,
		SupplierBillingFinal: facts.SupplierBillingFinal, NewSubmissionSafe: facts.NewSubmissionSafe,
		TemporaryTechnicalCause: facts.TemporaryTechnicalCause, PublicErrorCode: facts.PublicErrorCode,
		TaskLookupNotFound: facts.TaskLookupNotFound, ObservedAt: evidence.ObservedAt,
		FinalCustomerBillingReceiptID: facts.FinalCustomerBillingReceiptID,
		WatchUpstreamID:               facts.WatchUpstreamTaskID}
	if facts.TaskData != nil {
		verified.TaskData, err = common.Marshal(facts.TaskData)
		if err != nil || len(verified.TaskData) > 1<<20 {
			return model.TaskVerifiedReceipt{}, errors.New("receipt task data exceeds limit")
		}
	}
	if facts.State != nil {
		verified.PluginState, err = common.Marshal(facts.State)
		if err != nil || len(verified.PluginState) > 1<<20 {
			return model.TaskVerifiedReceipt{}, errors.New("receipt state exceeds limit")
		}
	}
	return verified, nil
}

func (a *TaskAdaptor) SendDurableTask(ctx context.Context, intake *model.TaskIntake, dispatch model.TaskDispatch) (*model.TaskHTTPObservation, error) {
	if dispatch.WireBaseURL != intake.SupplierBaseURL || dispatch.AccountID != intake.AccountID || dispatch.WireMethod != http.MethodPost {
		return nil, errors.New("frozen paid wire mismatch")
	}
	var headers map[string]string
	if err := common.Unmarshal(dispatch.WireHeaders, &headers); err != nil {
		return nil, err
	}
	for name := range headers {
		if strings.EqualFold(name, dispatch.WireKeyHeader) {
			return nil, errors.New("frozen wire contains attempt key header")
		}
	}
	base, err := url.Parse(dispatch.WireBaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" || !strings.HasPrefix(dispatch.WirePath, "/") {
		return nil, errors.New("frozen paid URL is invalid")
	}
	if dispatch.WireHost == "" ||
		(headers["host"] != "" && headers["host"] != dispatch.WireHost) ||
		(headers["host"] == "" && dispatch.WireHost != base.Host) {
		return nil, errors.New("frozen paid Host is inconsistent")
	}
	descriptor := requestDescriptor{URL: base.Scheme + "://" + base.Host + dispatch.WirePath,
		Method: dispatch.WireMethod, Headers: headers, Body: string(dispatch.WireBody),
		SupplierKeyHeader: dispatch.WireKeyHeader, WireHost: dispatch.WireHost}
	descriptor.Headers[dispatch.WireKeyHeader] = dispatch.SupplierKey
	var observation model.TaskHTTPObservation
	if dispatch.AtomicReplaySeconds > 0 {
		observation, err = a.durableDescriptorHTTPWithReceipt(ctx, descriptor, http.MethodPost, intake.SupplierBaseURL,
			"", a.info.ChannelSetting.Proxy, model.MaxTaskHTTPObservationBytes, model.MaxTaskHTTPObservationBytes,
			"", nil, model.MaxTaskSubmitAuditBodyBytes)
	} else {
		observation, err = a.durableDescriptorHTTP(ctx, descriptor, http.MethodPost, intake.SupplierBaseURL, "", a.info.ChannelSetting.Proxy, model.MaxTaskHTTPObservationBytes)
	}
	if err != nil {
		if observation.StatusCode == 0 {
			return nil, err
		}
		if len(observation.Headers) == 0 {
			observation.Headers = model.TaskBlob("{}")
		}
	}
	observation.Kind, observation.UpstreamID = "submit", dispatch.UpstreamID
	return &observation, nil
}

func (a *TaskAdaptor) ReadDurableOriginal(ctx context.Context, task *model.Task, intake *model.TaskIntake, dispatch model.TaskDispatch) (*model.TaskHTTPObservation, error) {
	if task == nil || intake == nil || dispatch.WireOperation != "adopt" || dispatch.AccountID != intake.AccountID ||
		dispatch.WireMethod != http.MethodPost || dispatch.WireKeyHeader != "idempotency-key" {
		return nil, errors.New("frozen original adoption is invalid")
	}
	var headers map[string]string
	if err := common.Unmarshal(dispatch.WireHeaders, &headers); err != nil {
		return nil, err
	}
	if headers["x-new-api-public-task"] != task.TaskID || headers["x-new-api-durable-proof"] == "" ||
		headers["idempotency-key"] != "" {
		return nil, errors.New("frozen original adoption proof is missing")
	}
	base, err := url.Parse(dispatch.WireBaseURL)
	if err != nil || base.Host == "" || !strings.HasPrefix(dispatch.WirePath, "/") ||
		dispatch.WireBaseURL != intake.SupplierBaseURL {
		return nil, errors.New("frozen original adoption URL is invalid")
	}
	descriptor := requestDescriptor{URL: base.Scheme + "://" + base.Host + dispatch.WirePath,
		Method: http.MethodPost, Headers: headers, Body: string(dispatch.WireBody)}
	observation, err := a.durableDescriptorHTTP(ctx, descriptor, http.MethodPost, intake.SupplierBaseURL,
		"", a.info.ChannelSetting.Proxy, model.MaxTaskHTTPObservationBytes)
	if err != nil {
		return nil, err
	}
	observation.Kind = "adopt"
	return &observation, nil
}

func (a *TaskAdaptor) QueryDurableTask(ctx context.Context, task *model.Task, intake *model.TaskIntake, dispatch model.TaskDispatch) (*model.TaskHTTPObservation, error) {
	if task == nil || intake == nil || dispatch.AccountID != intake.AccountID ||
		dispatch.WireBaseURL != intake.SupplierBaseURL || dispatch.UpstreamID == "" {
		return nil, errors.New("durable result query changed frozen account or task")
	}
	if dispatch.UncertainReplacementUsed && dispatch.WinnerOrdinal < 1 {
		return nil, errors.New("uncertain result query has no selected winner")
	}
	key, baseURL, proxy := a.queryCredentials()
	query, err := a.queryContext(task, key, baseURL, proxy)
	if err != nil {
		return nil, err
	}
	query["taskId"] = dispatch.UpstreamID
	descriptor, err := a.requestDescriptor(ctx, "buildQueryRequest", query)
	if err != nil {
		return nil, err
	}
	if descriptor.Method != "" && descriptor.Method != http.MethodGet || descriptor.Body != nil {
		return nil, errors.New("durable result query must be GET")
	}
	parsedURL, err := url.Parse(descriptor.URL)
	base, baseErr := url.Parse(intake.SupplierBaseURL)
	if err != nil || baseErr != nil || parsedURL.Host != base.Host || parsedURL.Scheme != base.Scheme {
		return nil, errors.New("durable result query crossed frozen account host")
	}
	observation, err := a.durableDescriptorHTTP(ctx, descriptor, http.MethodGet, baseURL, key, proxy, model.MaxTaskHTTPObservationBytes)
	if err != nil {
		return nil, err
	}
	observation.Kind, observation.UpstreamID = "query", dispatch.UpstreamID
	return &observation, nil
}

func (a *TaskAdaptor) QueryDurableAttempt(ctx context.Context, task *model.Task, intake *model.TaskIntake,
	attempt model.TaskDispatchAttempt, upstreamID string) (*model.TaskHTTPObservation, error) {
	if task == nil || intake == nil || attempt.TaskID != task.ID || attempt.Ordinal < 1 ||
		attempt.AccountID != intake.AccountID || attempt.RequestHash == "" || upstreamID == "" || len(upstreamID) > 191 {
		return nil, errors.New("historical task query identity is invalid")
	}
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
		return nil, err
	}
	if dispatch.Attempt < attempt.Ordinal || dispatch.AccountID != attempt.AccountID ||
		dispatch.RequestHash != attempt.RequestHash || dispatch.WireBaseURL != intake.SupplierBaseURL {
		return nil, errors.New("historical task query changed frozen wire")
	}
	if len(attempt.Evidence) > 0 {
		verified, err := a.VerifyDurableReceipt(ctx, task, intake, attempt, model.TaskReceipt{Evidence: attempt.Evidence})
		if err != nil || verified.Kind == "unknown" && verified.WatchUpstreamID != upstreamID ||
			verified.Kind == "accepted" && verified.UpstreamID != upstreamID ||
			verified.Kind != "unknown" && verified.Kind != "accepted" {
			return nil, errors.New("historical task query is not bound to receipt")
		}
	} else {
		if attempt.Ordinal != dispatch.Attempt || attempt.SupplierKey != dispatch.SupplierKey ||
			attempt.WatchUpstreamID != upstreamID || attempt.SubmitStatusCode/100 != 2 ||
			attempt.SubmitBodyTruncated || attempt.SubmitBodyReadError || len(attempt.SubmitBody) == 0 ||
			len(attempt.SubmitBody) > model.MaxTaskHTTPObservationBytes {
			return nil, errors.New("accepted task query has no complete bound submit")
		}
		if attempt.SubmitBodySHA256 != "" {
			digest := sha256.Sum256(attempt.SubmitBody)
			if hex.EncodeToString(digest[:]) != attempt.SubmitBodySHA256 {
				return nil, errors.New("accepted task submit body changed")
			}
		}
		parsed, err := a.ParseDurableSubmit(ctx, task, intake, model.TaskHTTPObservation{
			StatusCode: attempt.SubmitStatusCode, Headers: attempt.SubmitHeaders, Body: attempt.SubmitBody})
		if err != nil || parsed.UpstreamID != upstreamID {
			return nil, errors.New("accepted task query is not bound to submit")
		}
	}
	frozenTask := *task
	frozenTask.PrivateData.UpstreamTaskID = upstreamID
	key, baseURL, proxy := a.queryCredentials()
	if key == "" || baseURL != intake.SupplierBaseURL {
		return nil, errors.New("historical task query credentials changed")
	}
	query, err := a.queryContext(&frozenTask, key, baseURL, proxy)
	if err != nil {
		return nil, err
	}
	query["taskId"] = upstreamID
	descriptor, err := a.requestDescriptor(ctx, "buildDurableAttemptQueryRequest", query)
	if err != nil || descriptor.Method != http.MethodGet || descriptor.Body != nil || descriptor.SupplierKeyHeader != "" {
		return nil, errors.New("historical task query descriptor is invalid")
	}
	parsedURL, err := url.Parse(descriptor.URL)
	base, baseErr := url.Parse(baseURL)
	if err != nil || baseErr != nil || parsedURL.Scheme != base.Scheme || parsedURL.Host != base.Host ||
		descriptor.Headers["Authorization"] != "Bearer "+key {
		return nil, errors.New("historical task query crossed frozen account host")
	}
	observation, err := a.durableDescriptorHTTP(ctx, descriptor, http.MethodGet, baseURL, "", proxy, model.MaxTaskHTTPObservationBytes)
	if err != nil {
		return nil, err
	}
	observation.Kind, observation.UpstreamID = "query", frozenTask.PrivateData.UpstreamTaskID
	return &observation, nil
}

func (a *TaskAdaptor) ParseDurableCandidate(ctx context.Context, task *model.Task, intake *model.TaskIntake,
	observation model.TaskHTTPObservation) (service.DurableTaskResult, error) {
	var result service.DurableTaskResult
	if task == nil || intake == nil || observation.StatusCode != http.StatusOK ||
		observation.UpstreamID == "" || len(observation.UpstreamID) > 191 || len(observation.Body) == 0 {
		return result, errors.New("read-only candidate observation is invalid")
	}
	key, baseURL, proxy := a.queryCredentials()
	if key == "" || baseURL != intake.SupplierBaseURL {
		return result, errors.New("read-only candidate account changed")
	}
	query, err := a.queryContext(task, key, baseURL, proxy)
	if err != nil {
		return result, err
	}
	query["taskId"] = observation.UpstreamID
	var body any
	if err := common.Unmarshal(observation.Body, &body); err != nil {
		return result, err
	}
	value, err := a.plugin.Engine.Call(ctx, "parseDurableCandidate", query, body)
	if err != nil {
		return result, err
	}
	var parsed taskResult
	if err := convert(value, &parsed); err != nil {
		return result, err
	}
	result.UpstreamID, result.Status, result.Progress = observation.UpstreamID, model.TaskStatus(parsed.Status), parsed.Progress
	result.PublicErrorCode = parsed.PublicErrorCode
	if result.Status == model.TaskStatusFailure {
		var failure struct {
			FinalCustomerBillingReceiptID string `json:"finalCustomerBillingReceiptId"`
		}
		if err := convert(value, &failure); err != nil || failure.FinalCustomerBillingReceiptID == "" ||
			len(failure.FinalCustomerBillingReceiptID) > 191 || result.PublicErrorCode == "" {
			return service.DurableTaskResult{}, errors.New("read-only failure lacks final refund")
		}
		result.FinalCustomerBillingReceiptID = failure.FinalCustomerBillingReceiptID
	}
	result.Data = append([]byte(nil), observation.Body...)
	return result, nil
}

func (a *TaskAdaptor) ParseDurableSubmit(ctx context.Context, task *model.Task, intake *model.TaskIntake, observation model.TaskHTTPObservation) (service.DurableTaskResult, error) {
	var result service.DurableTaskResult
	if observation.StatusCode/100 != 2 {
		return result, fmt.Errorf("supplier submit HTTP status %d requires receipt reconciliation", observation.StatusCode)
	}
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
		return result, err
	}
	if len(dispatch.PreparedState) > 0 {
		if err := common.Unmarshal(dispatch.PreparedState, &a.prepareState); err != nil {
			return result, err
		}
	}
	c, info, err := a.durableSubmissionContext(ctx, task, intake)
	if err != nil {
		return result, err
	}
	value, err := a.plugin.Engine.Call(ctx, "parseSubmitResponse", a.submitContext(c, info), durableHookResponse(observation))
	if err != nil {
		return result, err
	}
	var parsed submitResponse
	if err := convert(value, &parsed); err != nil || parsed.TaskID == "" {
		return result, errors.New("durable supplier acceptance is invalid")
	}
	result.UpstreamID = parsed.TaskID
	if parsed.TaskData != nil {
		result.Data, err = common.Marshal(parsed.TaskData)
		if err != nil || len(result.Data) > model.MaxPendingTaskOutcomeBytes {
			return result, errors.New("durable submit data exceeds limit")
		}
	}
	if state, present := encodeReturnedPluginState(value); present {
		result.PluginState = state
	} else {
		result.PluginState = append([]byte(nil), task.PrivateData.PluginState...)
	}
	if len(result.PluginState) > maxTaskPluginPersistedJSONBytes {
		return result, errors.New("durable submit state exceeds limit")
	}
	if parsed.Immediate != nil {
		result.Status = model.TaskStatus(parsed.Immediate.Status)
		result.Progress, result.ResultURL = parsed.Immediate.Progress, parsed.Immediate.URL
		result.PublicErrorCode = parsed.Immediate.PublicErrorCode
		if result.Status == model.TaskStatusSuccess && a.hasHook(ctx, "extractUsageOnComplete") {
			query := map[string]any{"taskId": parsed.TaskID, "publicTaskId": task.TaskID, "data": parsed.TaskData, "state": parsed.State}
			facts, hookErr := a.plugin.Engine.Call(ctx, "extractUsageOnComplete", query, jsonValue(parsed.Immediate), parsed.TaskData)
			if hookErr != nil {
				return result, hookErr
			}
			result.UsageFacts, err = a.validatedCompletionUsageFacts(facts, task.Properties.UpstreamModelName, task.Properties.OriginModelName)
			if err != nil {
				return result, err
			}
		}
	} else {
		result.Status = model.TaskStatusSubmitted
	}
	return result, nil
}

func (a *TaskAdaptor) ParseDurableQuery(ctx context.Context, task *model.Task, intake *model.TaskIntake, observation model.TaskHTTPObservation) (service.DurableTaskResult, error) {
	var result service.DurableTaskResult
	if observation.StatusCode/100 != 2 {
		return result, fmt.Errorf("supplier query HTTP status %d", observation.StatusCode)
	}
	key, baseURL, proxy := a.queryCredentials()
	query, err := a.queryContext(task, key, baseURL, proxy)
	if err != nil {
		return result, err
	}
	if observation.UpstreamID != "" {
		query["taskId"] = observation.UpstreamID
	}
	input := durableHookResponse(observation)["body"]
	value, err := a.plugin.Engine.Call(ctx, "parseTaskResult", query, input, map[string]any{"status": observation.StatusCode, "headers": durableHookResponse(observation)["headers"]})
	if err != nil {
		return result, err
	}
	var parsed taskResult
	if err := convert(value, &parsed); err != nil {
		return result, err
	}
	result.UpstreamID, result.Status, result.Progress = observation.UpstreamID, model.TaskStatus(parsed.Status), parsed.Progress
	result.PublicErrorCode = parsed.PublicErrorCode
	result.ResultURL, result.Data = parsed.URL, append([]byte(nil), observation.Body...)
	if state, present := encodeReturnedPluginState(value); present {
		result.PluginState = state
	} else {
		result.PluginState = append([]byte(nil), task.PrivateData.PluginState...)
	}
	if len(result.PluginState) > maxTaskPluginPersistedJSONBytes {
		return result, errors.New("durable query state exceeds limit")
	}
	if result.Status == model.TaskStatusSuccess && a.hasHook(ctx, "extractUsageOnComplete") {
		facts, hookErr := a.plugin.Engine.Call(ctx, "extractUsageOnComplete", query, jsonValue(parsed), input)
		if hookErr != nil {
			return result, hookErr
		}
		result.UsageFacts, err = a.validatedCompletionUsageFacts(facts, task.Properties.UpstreamModelName, task.Properties.OriginModelName)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

var _ service.DurableTaskAdaptor = (*TaskAdaptor)(nil)
