package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/types"
	"gorm.io/gorm"
)

type DurableTaskPreparation struct {
	Wire       model.TaskPreparedWire
	Operation  string
	UsageFacts map[string]any
}

// DurableTaskPreparationRejection is a verified, permanent client input or
// content decision made before any supplier generation POST. A parser must
// supply a stable public code; provider prose is never a classification.
type DurableTaskPreparationRejection struct {
	Code   string
	Reason string
}

var ErrDurableFrozenInputMismatch = errors.New("frozen task input mismatch")
var ErrDurableFrozenContextInvalid = errors.New("frozen task context invalid")

func (e *DurableTaskPreparationRejection) Error() string { return e.Code }

type DurableTaskReceipt struct {
	Evidence         []byte
	Missing          bool
	Seen             bool
	RetryNotBeforeAt int64
}

type DurableTaskResult struct {
	UpstreamID                    string
	Status                        model.TaskStatus
	Progress                      string
	Data                          []byte
	PluginState                   []byte
	ResultURL                     string
	PublicErrorCode               string
	FinalCustomerBillingReceiptID string
	UsageFacts                    map[string]any
}

// The relay package supplies a frozen plugin adaptor at startup, avoiding a
// service -> relay import cycle. Every method uses bounded, cancellable I/O.
type DurableTaskAdaptor interface {
	PrepareDurableTask(context.Context, *model.Task, *model.TaskIntake) (DurableTaskPreparation, error)
	CheckDurableReceipt(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatch) (DurableTaskReceipt, error)
	VerifyDurableReceipt(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatchAttempt, model.TaskReceipt) (model.TaskVerifiedReceipt, error)
	CheckDurableAttemptReceipt(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatchAttempt) (DurableTaskReceipt, error)
	QueryDurableAttempt(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatchAttempt, string) (*model.TaskHTTPObservation, error)
	SendDurableTask(context.Context, *model.TaskIntake, model.TaskDispatch) (*model.TaskHTTPObservation, error)
	ReadDurableOriginal(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatch) (*model.TaskHTTPObservation, error)
	QueryDurableTask(context.Context, *model.Task, *model.TaskIntake, model.TaskDispatch) (*model.TaskHTTPObservation, error)
	ParseDurableSubmit(context.Context, *model.Task, *model.TaskIntake, model.TaskHTTPObservation) (DurableTaskResult, error)
	ParseDurableQuery(context.Context, *model.Task, *model.TaskIntake, model.TaskHTTPObservation) (DurableTaskResult, error)
	ParseDurableCandidate(context.Context, *model.Task, *model.TaskIntake, model.TaskHTTPObservation) (DurableTaskResult, error)
}

var GetDurableTaskAdaptorFunc func(context.Context, int64) (DurableTaskAdaptor, error)
var durableTaskCursor atomic.Int64
var uncertainOriginalCursor atomic.Int64
var uncertainFailureCursor atomic.Int64

func RunDurableTaskPass(ctx context.Context) bool {
	if GetDurableTaskAdaptorFunc == nil || ctx.Err() != nil {
		return false
	}
	cursor := durableTaskCursor.Load()
	ids, err := model.ListDueTaskDispatchIDs(ctx, cursor, 8)
	if err != nil {
		logger.LogError(ctx, "durable task dispatch page failed: %v", err)
		return false
	}
	if len(ids) == 0 {
		durableTaskCursor.Store(0)
		if err := runUncertainOriginalPass(ctx); err != nil {
			logger.LogError(ctx, "uncertain original reconciliation failed: %v", err)
			return false
		}
		if err := runUncertainFailurePass(ctx); err != nil {
			logger.LogError(ctx, "uncertain final failure recovery failed: %v", err)
			return false
		}
		return ctx.Err() == nil
	}
	durableTaskCursor.Store(ids[len(ids)-1])
	var workers sync.WaitGroup
	var failed atomic.Bool
	permits := make(chan struct{}, 4)
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		permits <- struct{}{}
		workers.Go(func() {
			defer func() { <-permits }()
			operationCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
			defer cancel()
			if err := processDurableTask(operationCtx, id); err != nil && !errors.Is(err, model.ErrTaskDispatchConflict) {
				logger.LogError(ctx, "durable task %d observation failed: %v", id, err)
				failed.Store(true)
			}
		})
	}
	workers.Wait()
	if err := runUncertainOriginalPass(ctx); err != nil {
		logger.LogError(ctx, "uncertain original reconciliation failed: %v", err)
		failed.Store(true)
	}
	if err := runUncertainFailurePass(ctx); err != nil {
		logger.LogError(ctx, "uncertain final failure recovery failed: %v", err)
		failed.Store(true)
	}
	return !failed.Load() && ctx.Err() == nil
}

func processDurableTask(ctx context.Context, taskID int64) error {
	var ownerBytes [16]byte
	if _, err := rand.Read(ownerBytes[:]); err != nil {
		return err
	}
	claim, err := model.ClaimTaskDispatch(ctx, taskID, hex.EncodeToString(ownerBytes[:]), 2*time.Minute)
	if err != nil {
		return err
	}
	deferred := true
	cleanupDelay := claim.ReconcileDelay
	defer func() {
		if deferred {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = model.DeferTaskDispatch(cleanupCtx, *claim, cleanupDelay)
		}
	}()
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&dispatch).Error; err != nil {
		return err
	}
	var task model.Task
	if err := model.DB.WithContext(ctx).Where("id = ?", taskID).Take(&task).Error; err != nil {
		return err
	}
	intake, err := model.GetTaskIntake(ctx, taskID)
	if err != nil {
		return err
	}
	adaptor, err := GetDurableTaskAdaptorFunc(ctx, taskID)
	if err != nil {
		if errors.Is(err, ErrDurableFrozenContextInvalid) {
			logger.LogError(ctx, "durable task %d frozen context validation failed: %v", taskID, err)
			if blockErr := model.BlockTaskDispatch(ctx, *claim, "frozen_context_invalid"); blockErr != nil {
				return blockErr
			}
			deferred = false
			return nil
		}
		return err
	}
	if dispatch.WinnerOrdinal > 0 {
		if err := completeUncertainWinner(ctx, adaptor, &task, intake, dispatch, *claim); err != nil {
			return err
		}
		deferred = false
		return nil
	}
	observation, err := model.GetTaskHTTPObservation(ctx, taskID)
	if err == nil {
		return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	switch dispatch.Phase {
	case model.TaskDispatchReady:
		if dispatch.RequestHash == "" {
			prepared, err := adaptor.PrepareDurableTask(ctx, &task, intake)
			if err != nil {
				if errors.Is(err, ErrDurableFrozenInputMismatch) {
					logger.LogError(ctx, "durable task %d frozen input verification failed: %v", taskID, err)
					if blockErr := model.BlockTaskDispatch(ctx, *claim, "frozen_input_mismatch"); blockErr != nil {
						return blockErr
					}
					deferred = false
					return nil
				}
				var rejected *DurableTaskPreparationRejection
				if errors.As(err, &rejected) && rejected.Code != "" && rejected.Reason != "" {
					deferred = false
					return settlePreparedTaskRejection(ctx, taskID, rejected.Code, rejected.Reason)
				}
				return err
			}
			if len(prepared.UsageFacts) > 0 {
				prepared.Wire.PreparedUsage, err = common.Marshal(prepared.UsageFacts)
				if err != nil {
					return err
				}
			}
			hash, err := model.PrepareTaskWire(ctx, *claim, prepared.Wire)
			if err != nil {
				return err
			}
			dispatch.RequestHash = hash
			if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&dispatch).Error; err != nil {
				return err
			}
		}
		if len(dispatch.PreparedUsage) > 0 {
			var facts map[string]any
			if err := common.Unmarshal(dispatch.PreparedUsage, &facts); err != nil {
				return err
			}
			quota, err := durableTaskQuota(intake, facts)
			if err != nil {
				return err
			}
			var finance model.TaskFinance
			if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&finance).Error; err != nil {
				return err
			}
			if quota > finance.ReservedQuota {
				if _, err := model.AdjustTaskReservation(ctx, taskID, finance.Version, quota, *claim); err != nil {
					if errors.Is(err, model.ErrTaskQuotaInsufficient) {
						deferred = false
						return settlePreparedTaskRejection(ctx, taskID, "insufficient_user_quota", "Insufficient quota for task preparation")
					}
					return err
				}
			}
		}
		if dispatch.WireOperation == "adopt" {
			claim, err = model.StartTaskAdoption(ctx, *claim, dispatch.RequestHash, intake.AccountID)
			if err != nil {
				return err
			}
			observation, err := adaptor.ReadDurableOriginal(ctx, &task, intake, dispatch)
			if err != nil {
				return err
			}
			observation.TaskID = taskID
			observation.Attempt, observation.ObservedAt = claim.Attempt, time.Now().Unix()
			if err := model.PersistTaskHTTPObservation(ctx, *claim, *observation); err != nil {
				return err
			}
			return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
		}
		probe, err := adaptor.CheckDurableReceipt(ctx, &task, intake, dispatch)
		if floorErr := model.PersistTaskReceiptRetryFloor(ctx, *claim, probe.RetryNotBeforeAt); floorErr != nil {
			return floorErr
		}
		if probe.Seen && dispatch.AtomicReplaySeconds > 0 {
			if blockErr := model.BlockTaskDispatch(ctx, *claim, "pre_send_receipt_seen", probe.Evidence); blockErr != nil {
				return blockErr
			}
			deferred = false
			return nil
		}
		if err != nil || !probe.Missing {
			if err != nil {
				return err
			}
			return errors.New("pre-send receipt is not authoritatively absent")
		}
		if dispatch.UncertainReplacementUsed {
			var original model.TaskDispatchAttempt
			if err := model.DB.WithContext(ctx).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).Take(&original).Error; err != nil {
				return err
			}
			fresh, err := adaptor.CheckDurableAttemptReceipt(ctx, &task, intake, original)
			if floorErr := model.PersistTaskReceiptRetryFloor(ctx, *claim, fresh.RetryNotBeforeAt); floorErr != nil {
				return floorErr
			}
			if floorErr := model.PersistAttemptReceiptRetryFloor(ctx, original, fresh.RetryNotBeforeAt); floorErr != nil {
				return floorErr
			}
			if err != nil {
				return err
			}
			if !fresh.Seen || len(fresh.Evidence) == 0 {
				return errors.New("original receipt unavailable before replacement send")
			}
			verified, err := adaptor.VerifyDurableReceipt(ctx, &task, intake, original, model.TaskReceipt{Evidence: fresh.Evidence})
			if err != nil {
				return err
			}
			if !model.QualifyUncertainReplacement(original, verified, fresh.Evidence) {
				deferred = false
				return model.CancelUncertainReplacement(ctx, *claim)
			}
			if fresh.RetryNotBeforeAt > time.Now().UnixMilli() {
				deferred = false
				return model.DeferTaskDispatch(ctx, *claim, time.Until(time.UnixMilli(fresh.RetryNotBeforeAt)))
			}
		}
		if probe.RetryNotBeforeAt > time.Now().UnixMilli() {
			deferred = false
			return model.DeferTaskDispatch(ctx, *claim, time.Until(time.UnixMilli(probe.RetryNotBeforeAt)))
		}
		claim, err = model.StartTaskSend(ctx, *claim, dispatch.RequestHash, intake.AccountID)
		if err != nil {
			return err
		}
		observation, err := adaptor.SendDurableTask(ctx, intake, dispatch)
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = model.MarkTaskSendUnknown(cleanupCtx, *claim)
			deferred = false
			return err
		}
		observation.TaskID = taskID
		observation.Attempt, observation.ObservedAt = claim.Attempt, time.Now().Unix()
		if err := model.PersistTaskHTTPObservation(ctx, *claim, *observation); err != nil {
			return err
		}
		return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
	case model.TaskDispatchAdopting:
		observation, err := adaptor.ReadDurableOriginal(ctx, &task, intake, dispatch)
		if err != nil {
			return err
		}
		observation.TaskID = taskID
		observation.Attempt, observation.ObservedAt = claim.Attempt, time.Now().Unix()
		if err := model.PersistTaskHTTPObservation(ctx, *claim, *observation); err != nil {
			return err
		}
		return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
	case model.TaskDispatchSendStarted, model.TaskDispatchUnknown:
		if dispatch.Phase == model.TaskDispatchUnknown && !dispatch.UncertainReplacementUsed && dispatch.UncertainRetryEnabled {
			var original model.TaskDispatchAttempt
			if err := model.DB.WithContext(ctx).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).Take(&original).Error; err != nil {
				return err
			}
			if original.ReceiptKind == "unknown" && original.WatchState == "pending" && dispatch.Attempt < dispatch.AttemptBudget {
				fresh, err := adaptor.CheckDurableAttemptReceipt(ctx, &task, intake, original)
				if floorErr := model.PersistTaskReceiptRetryFloor(ctx, *claim, fresh.RetryNotBeforeAt); floorErr != nil {
					return floorErr
				}
				if floorErr := model.PersistAttemptReceiptRetryFloor(ctx, original, fresh.RetryNotBeforeAt); floorErr != nil {
					return floorErr
				}
				if err != nil {
					return err
				}
				if fresh.Seen && len(fresh.Evidence) > 0 {
					verified, err := adaptor.VerifyDurableReceipt(ctx, &task, intake, original, model.TaskReceipt{Evidence: fresh.Evidence})
					if err != nil {
						return err
					}
					if model.QualifyUncertainReplacement(original, verified, fresh.Evidence) {
						minimum := time.UnixMilli(original.StartedAt + dispatch.UncertainRetryDelaySeconds*1000)
						if remaining := time.Until(minimum); remaining > 0 {
							deferred = false
							return model.DeferTaskDispatch(ctx, *claim, remaining)
						}
						deferred = false
						return model.AllocateUncertainReplacement(ctx, *claim, original, verified, fresh.Evidence)
					}
					deferred = false
					return model.ReconcileTaskReceipt(ctx, *claim, model.TaskReceipt{Evidence: fresh.Evidence},
						durableReceiptVerifier{adaptor: adaptor, task: &task, intake: intake})
				}
				deferred = false
				return model.DeferTaskDispatch(ctx, *claim, dispatch.ReconcileDelay())
			}
		}
		probe, err := adaptor.CheckDurableReceipt(ctx, &task, intake, dispatch)
		if floorErr := model.PersistTaskReceiptRetryFloor(ctx, *claim, probe.RetryNotBeforeAt); floorErr != nil {
			return floorErr
		}
		if err != nil {
			return err
		}
		if probe.Seen {
			if err := model.RecordTaskReceiptSeen(ctx, *claim, probe.Evidence); err != nil {
				return err
			}
		}
		if probe.Missing {
			if probe.RetryNotBeforeAt > time.Now().UnixMilli() {
				deferred = false
				return model.DeferTaskDispatch(ctx, *claim, time.Until(time.UnixMilli(probe.RetryNotBeforeAt)))
			}
			next, allowed, err := model.StartTaskReplay(ctx, *claim)
			if errors.Is(err, model.ErrTaskFrozenWireInvalid) {
				if blockErr := model.BlockTaskDispatch(ctx, *claim, "frozen_context_invalid"); blockErr != nil {
					return blockErr
				}
				deferred = false
				return nil
			}
			if err != nil {
				return err
			}
			if allowed {
				claim = next
				observation, err := adaptor.SendDurableTask(ctx, intake, dispatch)
				if err != nil {
					cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					defer cancel()
					_ = model.MarkTaskSendUnknown(cleanupCtx, *claim)
					deferred = false
					return err
				}
				observation.TaskID = taskID
				observation.Attempt, observation.ObservedAt = claim.Attempt, time.Now().Unix()
				if err := model.PersistTaskHTTPObservation(ctx, *claim, *observation); err != nil {
					return err
				}
				return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
			}
			deferred = false
			return model.DeferTaskDispatch(ctx, *claim, dispatch.ReconcileDelay())
		}
		if !probe.Seen || len(probe.Evidence) == 0 {
			deferred = false
			return model.DeferTaskDispatch(ctx, *claim, dispatch.ReconcileDelay())
		}
		verifier := durableReceiptVerifier{adaptor: adaptor, task: &task, intake: intake}
		if err := model.ReconcileTaskReceipt(ctx, *claim, model.TaskReceipt{Evidence: probe.Evidence}, verifier); err != nil {
			return err
		}
		deferred = false
		return nil
	case model.TaskDispatchAccepted:
		if dispatch.UncertainReplacementUsed {
			var attempt model.TaskDispatchAttempt
			if err := model.DB.WithContext(ctx).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).Take(&attempt).Error; err != nil {
				return err
			}
			readOnly, err := adaptor.QueryDurableAttempt(ctx, &task, intake, attempt, dispatch.UpstreamID)
			if err != nil {
				return err
			}
			if readOnly.StatusCode/100 != 2 {
				deferred = false
				return model.DeferTaskDispatch(ctx, *claim, durableRetryDelay(*readOnly, dispatch))
			}
			candidate, err := adaptor.ParseDurableCandidate(ctx, &task, intake, *readOnly)
			if err != nil {
				return err
			}
			if candidate.Status == model.TaskStatusSuccess && candidate.UpstreamID == dispatch.UpstreamID && len(candidate.Data) > 0 {
				deferred = false
				return model.SelectUncertainCandidate(ctx, taskID, dispatch.Attempt, candidate.UpstreamID, readOnly.Body)
			}
			if candidate.Status == model.TaskStatusFailure {
				deferred = false
				return model.ParkUncertainAttemptFailure(ctx, taskID, candidate.UpstreamID)
			}
			deferred = false
			return model.DeferTaskDispatch(ctx, *claim, dispatch.ReconcileDelay())
		}
		observation, err := adaptor.QueryDurableTask(ctx, &task, intake, dispatch)
		if err != nil {
			return err
		}
		observation.TaskID = taskID
		observation.Attempt, observation.ObservedAt = claim.Attempt, time.Now().Unix()
		if err := model.PersistTaskHTTPObservation(ctx, *claim, *observation); err != nil {
			return err
		}
		return completeDurableObservation(ctx, adaptor, &task, intake, *claim, *observation)
	case model.TaskDispatchRejected:
		if dispatch.UncertainReplacementUsed {
			if err := maybeSettleUncertainFailures(ctx, taskID, intake); err != nil {
				return err
			}
			var finance model.TaskFinance
			if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&finance).Error; err != nil {
				return err
			}
			if finance.State == "settled" {
				deferred = false
				return nil
			}
			deferred = false
			return model.DeferTaskDispatch(ctx, *claim, time.Hour)
		}
		var attempt model.TaskDispatchAttempt
		if err := model.DB.WithContext(ctx).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).Take(&attempt).Error; err != nil {
			return err
		}
		if (attempt.ReceiptKind != "rejected" && attempt.ReceiptKind != "not_accepted") ||
			!attempt.SupplierBillingFinal || attempt.SupplierNetQuota != 0 || attempt.PublicErrorCode == "" ||
			(attempt.ReceiptKind == "not_accepted" && (!attempt.NewSubmissionSafe || !attempt.TemporaryTechnicalCause)) {
			return model.ErrTaskDispatchConflict
		}
		var finance model.TaskFinance
		if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&finance).Error; err != nil {
			return err
		}
		var routing DurableTaskRoutingSnapshot
		if err := common.Unmarshal(intake.RoutingSnapshot, &routing); err != nil {
			if blockErr := model.BlockTaskDispatch(ctx, *claim, "frozen_context_invalid"); blockErr != nil {
				return blockErr
			}
			deferred = false
			return nil
		}
		reason := "Task rejected by provider"
		if attempt.ReceiptKind == "not_accepted" {
			reason = "Task could not be accepted by provider"
		}
		if attempt.PublicErrorCode == "reference_unavailable" || attempt.PublicErrorCode == "reference_not_ready" {
			reason = model.TaskPublicFailureMessage(attempt.PublicErrorCode)
		}
		outcome := model.TaskOutcome{Status: model.TaskStatusFailure, ActualQuota: 0,
			ErrorCode: attempt.PublicErrorCode, FailReason: reason, RetainResult: routing.RetainResult}
		if err := model.PersistTaskOutcome(ctx, taskID, finance.Version, outcome); err != nil {
			return err
		}
		return model.CommitTaskOutcome(ctx, taskID, finance.Version, outcome)
	}
	deferred = false
	return nil
}

func settlePreparedTaskRejection(ctx context.Context, taskID int64, code, reason string) error {
	var finance model.TaskFinance
	if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&finance).Error; err != nil {
		return err
	}
	outcome := model.TaskOutcome{Status: model.TaskStatusFailure, ActualQuota: 0,
		ErrorCode: code, FailReason: reason, RetainResult: true}
	if err := model.PersistTaskOutcome(ctx, taskID, finance.Version, outcome); err != nil {
		return err
	}
	return model.CommitTaskOutcome(ctx, taskID, finance.Version, outcome)
}

type durableReceiptVerifier struct {
	adaptor DurableTaskAdaptor
	task    *model.Task
	intake  *model.TaskIntake
}

func (v durableReceiptVerifier) VerifyTaskReceipt(ctx context.Context, attempt model.TaskDispatchAttempt, receipt model.TaskReceipt) (model.TaskVerifiedReceipt, error) {
	return v.adaptor.VerifyDurableReceipt(ctx, v.task, v.intake, attempt, receipt)
}

func durableTaskQuota(intake *model.TaskIntake, facts map[string]any) (int, error) {
	var snapshot DurableTaskBillingSnapshot
	if err := common.Unmarshal(intake.BillingSnapshot, &snapshot); err != nil {
		return 0, err
	}
	if snapshot.Context.TieredSnapshot != nil {
		result, _, err := EvaluateTaskCompletionUsage(snapshot.Context.TieredSnapshot, facts)
		if err != nil || result.Clamp != nil {
			return 0, errors.New("frozen task billing facts are invalid")
		}
		return result.ActualQuotaAfterGroup, nil
	}
	if len(facts) == 0 {
		return snapshot.Quota, nil
	}
	price := &types.PriceData{}
	if len(snapshot.Context.OtherRatios) > 0 && !price.ReplaceOtherRatios(snapshot.Context.OtherRatios) {
		return 0, errors.New("frozen task ratio snapshot is invalid")
	}
	base := price.RemoveOtherRatiosFromFloat(float64(snapshot.Quota))
	ratios := price.OtherRatios()
	if ratios == nil {
		ratios = make(map[string]float64)
	}
	for key, value := range facts {
		ratio, ok := value.(float64)
		if !ok || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
			return 0, errors.New("prepared task ratio is invalid")
		}
		ratios[key] = ratio
	}
	if !price.ReplaceOtherRatios(ratios) {
		return 0, errors.New("prepared task ratios are invalid")
	}
	quota, clamp := common.QuotaFromFloatChecked(price.ApplyOtherRatiosToFloat(base))
	if clamp != nil {
		return 0, errors.New("prepared task quota exceeded limit")
	}
	return quota, nil
}

func completeDurableObservation(ctx context.Context, adaptor DurableTaskAdaptor, task *model.Task, intake *model.TaskIntake,
	claim model.TaskDispatchClaim, observation model.TaskHTTPObservation) error {
	var timing model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&timing).Error; err != nil {
		return err
	}
	if observation.Kind == "submit" {
		var dispatch model.TaskDispatch
		if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.AtomicReplaySeconds > 0 {
			if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
				return err
			}
			probe, err := adaptor.CheckDurableReceipt(ctx, task, intake, dispatch)
			if floorErr := model.PersistTaskReceiptRetryFloor(ctx, claim, probe.RetryNotBeforeAt); floorErr != nil {
				return floorErr
			}
			if err != nil {
				return err
			}
			if probe.Seen {
				if err := model.RecordTaskReceiptSeen(ctx, claim, probe.Evidence); err != nil {
					return err
				}
			}
			if probe.Seen && len(probe.Evidence) > 0 {
				return model.ReconcileTaskReceipt(ctx, claim, model.TaskReceipt{Evidence: probe.Evidence},
					durableReceiptVerifier{adaptor: adaptor, task: task, intake: intake})
			}
			return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(observation, timing))
		}
	}
	if observation.Kind == "adopt" && observation.StatusCode/100 != 2 {
		if observation.StatusCode == http.StatusUnauthorized || observation.StatusCode == http.StatusForbidden {
			if err := model.BlockTaskDispatch(ctx, claim, "adoption_authorization_failed"); err != nil {
				return err
			}
			return nil
		}
		if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
			return err
		}
		return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(observation, timing))
	}
	if observation.Kind == "submit" && observation.StatusCode/100 != 2 {
		if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
			return err
		}
		if claim.Phase == model.TaskDispatchSendStarted {
			if err := model.MarkTaskSendUnknown(ctx, claim); err != nil {
				return err
			}
		}
		return nil
	}
	if observation.Kind == "query" && observation.StatusCode/100 != 2 {
		if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
			return err
		}
		return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(observation, timing))
	}
	var result DurableTaskResult
	var err error
	if observation.Kind == "submit" || observation.Kind == "adopt" {
		result, err = adaptor.ParseDurableSubmit(ctx, task, intake, observation)
	} else {
		result, err = adaptor.ParseDurableQuery(ctx, task, intake, observation)
	}
	if err != nil {
		return err
	}
	if observation.Kind == "submit" || observation.Kind == "adopt" {
		if result.UpstreamID == "" {
			return errors.New("supplier acceptance identity is missing")
		}
		terminal := result.Status == model.TaskStatusSuccess || result.Status == model.TaskStatusFailure
		if !terminal && result.Status != model.TaskStatusSubmitted && result.Status != model.TaskStatusQueued && result.Status != model.TaskStatusInProgress {
			return errors.New("supplier submit returned an unknown status")
		}
		if claim.Phase == model.TaskDispatchSendStarted || claim.Phase == model.TaskDispatchAdopting {
			// Terminal data is published only by the finance/outcome commit.
			publishData := !terminal
			if err := model.AcceptTaskDispatch(ctx, claim, result.UpstreamID, result.Data, result.PluginState,
				publishData, !terminal || timing.UncertainReplacementUsed); err != nil {
				return err
			}
			if !terminal {
				return nil
			}
		} else if claim.Phase != model.TaskDispatchAccepted {
			return model.ErrTaskDispatchConflict
		}
		if !terminal {
			return model.ClearTaskHTTPObservation(ctx, claim, observation)
		}
	}
	if result.Status != model.TaskStatusSuccess && result.Status != model.TaskStatusFailure {
		if result.Status != model.TaskStatusSubmitted && result.Status != model.TaskStatusQueued && result.Status != model.TaskStatusInProgress {
			return errors.New("supplier query returned an unknown status")
		}
		if err := model.UpdateAcceptedTaskProgress(ctx, claim, result.Status, result.Progress, result.PluginState); err != nil {
			return err
		}
		if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
			return err
		}
		return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(observation, timing))
	}
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
		return err
	}
	if dispatch.Phase != model.TaskDispatchAccepted || dispatch.UpstreamID != result.UpstreamID {
		return model.ErrTaskDispatchConflict
	}
	if dispatch.UncertainReplacementUsed {
		if result.Status == model.TaskStatusFailure {
			return model.ParkUncertainAttemptFailure(ctx, task.ID, result.UpstreamID)
		}
		return nil
	}
	outcome, version, err := durableOutcomeFromResult(ctx, task.ID, intake, dispatch, result)
	if err != nil {
		return err
	}
	if err := model.PersistTaskOutcome(ctx, task.ID, version, outcome); err != nil {
		return err
	}
	if err := model.ClearTaskHTTPObservation(ctx, claim, observation); err != nil {
		return err
	}
	return model.CommitTaskOutcome(ctx, task.ID, version, outcome)
}

func durableOutcomeFromResult(ctx context.Context, taskID int64, intake *model.TaskIntake,
	dispatch model.TaskDispatch, result DurableTaskResult) (model.TaskOutcome, int64, error) {
	var err error
	var finance model.TaskFinance
	if err := model.DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&finance).Error; err != nil {
		return model.TaskOutcome{}, 0, err
	}
	var routing DurableTaskRoutingSnapshot
	if err := common.Unmarshal(intake.RoutingSnapshot, &routing); err != nil {
		return model.TaskOutcome{}, 0, err
	}
	mergedFacts := make(map[string]any)
	if len(dispatch.PreparedUsage) > 0 {
		if err := common.Unmarshal(dispatch.PreparedUsage, &mergedFacts); err != nil {
			return model.TaskOutcome{}, 0, err
		}
	}
	for key, value := range result.UsageFacts {
		mergedFacts[key] = value
	}
	quota := 0
	if result.Status == model.TaskStatusSuccess {
		quota, err = durableTaskQuota(intake, mergedFacts)
		if err != nil {
			return model.TaskOutcome{}, 0, err
		}
	}
	if quota < 0 || quota > math.MaxInt32 {
		return model.TaskOutcome{}, 0, fmt.Errorf("invalid durable completion quota")
	}
	if result.Status == model.TaskStatusFailure && result.PublicErrorCode == "" {
		return model.TaskOutcome{}, 0, errors.New("supplier failure lacks a typed public code")
	}
	outcome := model.TaskOutcome{Status: result.Status, ActualQuota: quota, TaskID: result.UpstreamID,
		Data: result.Data, ResultURL: result.ResultURL, PluginState: result.PluginState,
		RetainResult: routing.RetainResult, ErrorCode: result.PublicErrorCode, FailReason: "Task failed"}
	if result.PublicErrorCode == "reference_unavailable" || result.PublicErrorCode == "reference_not_ready" {
		outcome.FailReason = model.TaskPublicFailureMessage(result.PublicErrorCode)
	}
	if len(mergedFacts) > 0 {
		outcome.UsageFacts, err = common.Marshal(mergedFacts)
		if err != nil {
			return model.TaskOutcome{}, 0, err
		}
	}
	if result.Status == model.TaskStatusSuccess {
		outcome.ErrorCode, outcome.FailReason = "", ""
	}
	return outcome, finance.Version, nil
}

func completeUncertainWinner(ctx context.Context, adaptor DurableTaskAdaptor, task *model.Task,
	intake *model.TaskIntake, dispatch model.TaskDispatch, claim model.TaskDispatchClaim) error {
	if !dispatch.UncertainReplacementUsed || dispatch.WinnerOrdinal < 1 ||
		dispatch.Phase != model.TaskDispatchAccepted || dispatch.UpstreamID == "" {
		return model.ErrTaskDispatchConflict
	}
	observation, err := adaptor.QueryDurableTask(ctx, task, intake, dispatch)
	if err != nil {
		return err
	}
	if observation.StatusCode/100 != 2 {
		return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(*observation, dispatch))
	}
	result, err := adaptor.ParseDurableQuery(ctx, task, intake, *observation)
	if err != nil {
		return err
	}
	if result.Status != model.TaskStatusSuccess || result.UpstreamID != dispatch.UpstreamID || len(result.Data) == 0 {
		if result.UpstreamID != dispatch.UpstreamID || result.Status == model.TaskStatusFailure {
			return errors.New("selected task archive changed its confirmed result")
		}
		return model.DeferTaskDispatch(ctx, claim, durableRetryDelay(*observation, dispatch))
	}
	outcome, version, err := durableOutcomeFromResult(ctx, task.ID, intake, dispatch, result)
	if err != nil {
		return err
	}
	if err := model.PersistTaskOutcome(ctx, task.ID, version, outcome); err != nil {
		return err
	}
	return model.CommitTaskOutcome(ctx, task.ID, version, outcome)
}

func runUncertainOriginalPass(ctx context.Context) error {
	cursor := uncertainOriginalCursor.Load()
	attempts, err := model.ListDueUncertainOriginals(ctx, cursor, 8)
	if err != nil {
		return err
	}
	if len(attempts) == 0 {
		uncertainOriginalCursor.Store(0)
		return nil
	}
	uncertainOriginalCursor.Store(attempts[len(attempts)-1].TaskID)
	for _, attempt := range attempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		operationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := reconcileUncertainAttempt(operationCtx, attempt)
		cancel()
		if err != nil && !errors.Is(err, model.ErrTaskDispatchConflict) {
			logger.LogError(ctx, "uncertain original %d/%d observation failed: %v", attempt.TaskID, attempt.Ordinal, err)
			if deferErr := model.DeferUncertainOriginal(ctx, attempt, time.Minute); deferErr != nil && !errors.Is(deferErr, model.ErrTaskDispatchConflict) {
				return deferErr
			}
		}
	}
	return nil
}

func reconcileUncertainAttempt(ctx context.Context, attempt model.TaskDispatchAttempt) error {
	var dispatch model.TaskDispatch
	if err := model.DB.WithContext(ctx).Where("task_id = ?", attempt.TaskID).Take(&dispatch).Error; err != nil {
		return err
	}
	if !dispatch.UncertainReplacementUsed {
		return model.ErrTaskDispatchConflict
	}
	var task model.Task
	if err := model.DB.WithContext(ctx).Where("id = ?", attempt.TaskID).Take(&task).Error; err != nil {
		return err
	}
	intake, err := model.GetTaskIntake(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	adaptor, err := GetDurableTaskAdaptorFunc(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	if attempt.ReceiptKind != "accepted" || attempt.WatchUpstreamID == "" {
		fresh, err := adaptor.CheckDurableAttemptReceipt(ctx, &task, intake, attempt)
		if floorErr := model.PersistAttemptReceiptRetryFloor(ctx, attempt, fresh.RetryNotBeforeAt); floorErr != nil {
			return floorErr
		}
		if err != nil {
			return err
		}
		if fresh.RetryNotBeforeAt > time.Now().UnixMilli() {
			return model.DeferUncertainOriginal(ctx, attempt, time.Until(time.UnixMilli(fresh.RetryNotBeforeAt)))
		}
		if fresh.Seen && len(fresh.Evidence) > 0 {
			verified, verifyErr := adaptor.VerifyDurableReceipt(ctx, &task, intake, attempt,
				model.TaskReceipt{Evidence: fresh.Evidence})
			if verifyErr != nil {
				return verifyErr
			}
			if verified.Kind == "rejected" || verified.Kind == "not_accepted" {
				if err := model.AuditUncertainRefusal(ctx, attempt, verified, fresh.Evidence); err != nil {
					return err
				}
				return maybeSettleUncertainFailures(ctx, attempt.TaskID, intake)
			}
			if verified.Kind == "accepted" || verified.Kind == "unknown" && verified.WatchUpstreamID != "" {
				if err := model.RefreshUncertainWatchIdentity(ctx, attempt, verified, fresh.Evidence); err != nil {
					return err
				}
				if attempt.WatchUpstreamID == "" {
					return model.DeferUncertainOriginal(ctx, attempt, dispatch.ReconcileDelay())
				}
			}
		}
	}
	if attempt.WatchUpstreamID == "" || (len(attempt.Evidence) == 0 && attempt.SubmitObservedAt == 0) {
		return model.DeferUncertainOriginal(ctx, attempt, dispatch.ReconcileDelay())
	}
	observation, err := adaptor.QueryDurableAttempt(ctx, &task, intake, attempt, attempt.WatchUpstreamID)
	if err != nil {
		return err
	}
	if observation.StatusCode/100 != 2 {
		return model.DeferUncertainOriginal(ctx, attempt, durableRetryDelay(*observation, dispatch))
	}
	result, err := adaptor.ParseDurableCandidate(ctx, &task, intake, *observation)
	if err != nil {
		return err
	}
	if result.UpstreamID != attempt.WatchUpstreamID {
		return model.ErrTaskDispatchConflict
	}
	if result.Status == model.TaskStatusSuccess && len(result.Data) > 0 {
		if task.Status != model.TaskStatusSuccess && task.Status != model.TaskStatusFailure && dispatch.WinnerOrdinal == 0 {
			err = model.SelectUncertainCandidate(ctx, attempt.TaskID, attempt.Ordinal, result.UpstreamID, observation.Body)
			if err != nil && !errors.Is(err, model.ErrTaskDispatchConflict) {
				return err
			}
			if errors.Is(err, model.ErrTaskDispatchConflict) {
				var current model.TaskDispatch
				if readErr := model.DB.WithContext(ctx).Where("task_id = ?", attempt.TaskID).Take(&current).Error; readErr != nil {
					return readErr
				}
				if current.WinnerOrdinal == 0 {
					return model.DeferUncertainOriginal(ctx, attempt, current.ReconcileDelay())
				}
			}
		}
		return model.AuditUncertainOriginal(ctx, attempt, result.Status, result.UpstreamID, "", "", observation.Body)
	}
	if result.Status == model.TaskStatusFailure {
		if err := model.AuditUncertainOriginal(ctx, attempt, result.Status, result.UpstreamID,
			result.PublicErrorCode, result.FinalCustomerBillingReceiptID, observation.Body); err != nil {
			return err
		}
		return maybeSettleUncertainFailures(ctx, attempt.TaskID, intake)
	}
	return model.DeferUncertainOriginal(ctx, attempt, dispatch.ReconcileDelay())
}

func maybeSettleUncertainFailures(ctx context.Context, taskID int64, intake *model.TaskIntake) error {
	var routing DurableTaskRoutingSnapshot
	if err := common.Unmarshal(intake.RoutingSnapshot, &routing); err != nil {
		return err
	}
	outcome, version, err := model.PersistUncertainFinalFailure(ctx, taskID, routing.RetainResult)
	if errors.Is(err, model.ErrTaskDispatchConflict) {
		return nil
	}
	if err != nil {
		return err
	}
	return model.CommitTaskOutcome(ctx, taskID, version, outcome)
}

func runUncertainFailurePass(ctx context.Context) error {
	cursor := uncertainFailureCursor.Load()
	ids, err := model.ListUnsettledUncertainFailures(ctx, cursor, 8)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		uncertainFailureCursor.Store(0)
		return nil
	}
	uncertainFailureCursor.Store(ids[len(ids)-1])
	for _, taskID := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		intake, err := model.GetTaskIntake(ctx, taskID)
		if err != nil {
			return err
		}
		if err := maybeSettleUncertainFailures(ctx, taskID, intake); err != nil {
			return err
		}
	}
	return nil
}

func durableRetryDelay(observation model.TaskHTTPObservation, dispatch model.TaskDispatch) time.Duration {
	base := dispatch.ReconcileDelay()
	observedAt := time.Unix(observation.ObservedAt, 0)
	var headers http.Header
	if common.Unmarshal(observation.Headers, &headers) == nil {
		if seconds, err := strconv.ParseInt(headers.Get("Retry-After"), 10, 64); err == nil && seconds > 0 {
			return max(base, time.Until(observedAt.Add(time.Duration(min(seconds, 3600))*time.Second)))
		}
		if deadline, err := http.ParseTime(headers.Get("Retry-After")); err == nil {
			cutoff := observedAt.Add(time.Hour)
			if deadline.After(cutoff) {
				deadline = cutoff
			}
			return max(base, time.Until(deadline))
		}
	}
	return base
}
