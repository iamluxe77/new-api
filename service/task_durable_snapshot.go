package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"gorm.io/gorm"
)

// DurableTaskRoutingSnapshot is private task input for the native worker and
// frozen response presenter. It is never embedded in a public task DTO.
type DurableTaskRoutingSnapshot struct {
	Surface          string                      `json:"surface"`
	InputEncoding    string                      `json:"input_encoding,omitempty"`
	BodyModelRewrite string                      `json:"body_model_rewrite,omitempty"`
	Protocol         string                      `json:"protocol,omitempty"`
	Operation        string                      `json:"operation,omitempty"`
	RouteRender      string                      `json:"route_render,omitempty"`
	RouteDecode      string                      `json:"route_decode,omitempty"`
	RetainResult     bool                        `json:"retain_result"`
	RouteRequest     json.RawMessage             `json:"route_request,omitempty"`
	ProtocolRequest  json.RawMessage             `json:"protocol_request,omitempty"`
	ChannelType      int                         `json:"channel_type"`
	ChannelID        int                         `json:"channel_id"`
	OriginModel      string                      `json:"origin_model"`
	UpstreamModel    string                      `json:"upstream_model"`
	Action           string                      `json:"action"`
	OriginTasks      []relaycommon.OriginTaskRef `json:"origin_tasks,omitempty"`
}

// DurableTaskNormalizedMarker binds the large decoded request to its single
// frozen raw copy. The worker recomputes all digests before preparation I/O.
type DurableTaskNormalizedMarker struct {
	Kind          string `json:"kind"`
	SHA256        string `json:"sha256"`
	Model         string `json:"model"`
	Action        string `json:"action"`
	WebhookSHA256 string `json:"webhook_sha256"`
}

// DurableTaskCredentialSnapshot is private. The selected credential and all
// channel settings needed to rebuild a worker adaptor live here, never in the
// public Task row or routing snapshot.
type DurableTaskCredentialSnapshot struct {
	Channel relaycommon.ChannelMeta `json:"channel"`
}

type DurableTaskBillingSnapshot struct {
	Context      model.TaskBillingContext `json:"context"`
	Quota        int                      `json:"quota"`
	QuotaPerUnit float64                  `json:"quota_per_unit"`
	Group        string                   `json:"group"`
	TokenID      int                      `json:"token_id"`
	ModelName    string                   `json:"model_name"`
}

// LoadFrozenTaskPlugin never consults the current registry or plugin row.
// A corrupt source/hash mismatch fails observation without creating work.
func LoadFrozenTaskPlugin(ctx context.Context, taskID int64) (*model.TaskIntake, *jsplugin.LoadedPlugin, error) {
	intake, err := model.GetTaskIntake(ctx, taskID)
	if err != nil {
		return nil, nil, err
	}
	digest := sha256.Sum256(intake.PluginSource)
	if len(intake.PluginSource) == 0 || len(intake.PluginSource) > model.MaxTaskPluginSourceBytes ||
		hex.EncodeToString(digest[:]) != intake.PluginSourceHash {
		return nil, nil, fmt.Errorf("%w: plugin source is unavailable", ErrDurableFrozenContextInvalid)
	}
	plugin, err := jsplugin.CompilePlugin(string(intake.PluginSource), jsplugin.Options{})
	if err != nil || plugin.Meta.Key != intake.PluginKey || plugin.Meta.Version != intake.PluginVersion ||
		plugin.Meta.APIVersion != intake.PluginAPIVersion || !plugin.Meta.DurableSubmit() {
		return nil, nil, fmt.Errorf("%w: plugin validation failed", ErrDurableFrozenContextInvalid)
	}
	return intake, plugin, nil
}

func DecodeDurableRoutingSnapshot(intake *model.TaskIntake) (DurableTaskRoutingSnapshot, error) {
	var snapshot DurableTaskRoutingSnapshot
	if intake == nil || len(intake.RoutingSnapshot) == 0 {
		return snapshot, errors.New("durable task routing snapshot is missing")
	}
	if err := common.Unmarshal(intake.RoutingSnapshot, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.Surface == "" || snapshot.ChannelID != intake.ChannelID || snapshot.ChannelID <= 0 {
		return snapshot, errors.New("durable task routing snapshot is invalid")
	}
	return snapshot, nil
}

// DurableTaskDelivery keeps a non-retained final payload private. The caller
// must already have authenticated and checked ownership of the public Task.
func DurableTaskDelivery(ctx context.Context, task *model.Task, retainResult bool) (*model.Task, *model.PendingTaskOutcome, error) {
	if task == nil {
		return nil, nil, errors.New("task is missing")
	}
	if retainResult && len(task.Data) > 0 {
		return task, nil, nil
	}
	var outcome model.PendingTaskOutcome
	err := model.DB.WithContext(ctx).Where("task_id = ? AND state = ?", task.ID, "settled").First(&outcome).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if !retainResult && (task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure) {
			return nil, nil, errors.New("private task outcome is unavailable")
		}
		return task, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if outcome.Status != task.Status {
		return nil, nil, errors.New("task delivery outcome disagrees with public status")
	}
	copy := *task
	if !retainResult && outcome.Status == model.TaskStatusSuccess {
		copy.Data = append(copy.Data[:0:0], outcome.Data...)
		copy.PrivateData.ResultDiscarded = false
	}
	return &copy, &outcome, nil
}
