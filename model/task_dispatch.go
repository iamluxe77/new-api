package model

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

const (
	TaskDispatchReady       = "ready"
	TaskDispatchSendStarted = "send_started"
	TaskDispatchAdopting    = "adopting"
	TaskDispatchUnknown     = "unknown"
	TaskDispatchAccepted    = "accepted"
	TaskDispatchRejected    = "rejected"
	TaskDispatchParked      = "parked"
	TaskDispatchBlocked     = "blocked"
	DefaultTaskAttempts     = 2
	MaxTaskAttemptBudget    = 8
	DefaultTaskBackoff      = 1000
)

var ErrTaskDispatchConflict = errors.New("task dispatch claim or receipt conflict")
var ErrTaskFrozenWireInvalid = errors.New("frozen task wire invalid")

type TaskDispatch struct {
	TaskID                        int64 `gorm:"primaryKey;autoIncrement:false"`
	Version                       int64
	Phase                         string `gorm:"type:varchar(24);index"`
	StageAt                       int64
	Owner                         string `gorm:"type:varchar(128)"`
	LeaseUntil                    int64  `gorm:"index"`
	Attempt                       int
	AttemptBudget                 int
	BackoffMillis                 int64
	RetryPolicyVersion            int
	ReconcileDelaySeconds         int64
	ReplayInitialDelaySeconds     int64
	ReplayMaxDelaySeconds         int64
	ReplayJitterSeconds           int64
	UncertainRetryEnabled         bool
	UncertainRetryDelaySeconds    int64
	UncertainReplacementUsed      bool
	UncertainReplacementCancelled bool
	WinnerOrdinal                 int
	ResumeReason                  string     `gorm:"type:varchar(255)"`
	BlockedReason                 string     `gorm:"type:varchar(64)"`
	BlockedFrom                   string     `gorm:"type:varchar(24)"`
	BlockedTaskStatus             TaskStatus `json:"-" gorm:"type:varchar(24)"`
	BlockedTaskProgress           string     `json:"-" gorm:"type:varchar(64)"`
	PreSendReceiptEvidence        TaskBlob   `json:"-"`
	SupplierKey                   string     `json:"-" gorm:"type:varchar(128)"`
	UpstreamID                    string     `gorm:"type:varchar(191)"`
	RequestHash                   string     `json:"-" gorm:"type:char(64)"`
	WireOperation                 string     `json:"-" gorm:"type:varchar(16)"`
	AtomicReplaySeconds           int64      `json:"-"`
	AtomicReplayGuaranteedSeconds int64      `json:"-"`
	RoutingHash                   string     `json:"-" gorm:"type:char(64)"`
	WireBaseURL                   string     `json:"-" gorm:"type:varchar(2048)"`
	WireHost                      string     `json:"-" gorm:"type:varchar(255)"`
	WireKeyHeader                 string     `json:"-" gorm:"type:varchar(128)"`
	WireHeaders                   TaskBlob   `json:"-"`
	WireMethod                    string     `json:"-" gorm:"type:varchar(12)"`
	WirePath                      string     `json:"-" gorm:"type:varchar(512)"`
	WireType                      string     `json:"-" gorm:"type:varchar(255)"`
	WireBody                      TaskBlob   `json:"-"`
	PreparedUsage                 TaskBlob   `json:"-"`
	PreparedState                 TaskBlob   `json:"-"`
	AccountID                     string     `json:"-" gorm:"type:varchar(191)"`
	NextAttemptAt                 int64
}

type TaskRetryPolicy struct {
	AttemptBudget              int
	BackoffMillis              int64
	ReconcileDelaySeconds      int64
	ReplayInitialDelaySeconds  int64
	ReplayMaxDelaySeconds      int64
	ReplayJitterSeconds        int64
	UncertainRetryEnabled      bool
	UncertainRetryDelaySeconds int64
}

func normalizeTaskRetryPolicy(policy TaskRetryPolicy) (TaskRetryPolicy, error) {
	legacyDefault := policy == (TaskRetryPolicy{})
	if policy.AttemptBudget == 0 {
		policy.AttemptBudget = DefaultTaskAttempts
	}
	if policy.BackoffMillis == 0 {
		policy.BackoffMillis = DefaultTaskBackoff
	}
	if policy.ReconcileDelaySeconds == 0 {
		policy.ReconcileDelaySeconds = 15
	}
	if policy.ReplayInitialDelaySeconds == 0 {
		policy.ReplayInitialDelaySeconds = 60
	}
	if policy.ReplayMaxDelaySeconds == 0 {
		policy.ReplayMaxDelaySeconds = 3840
	}
	if legacyDefault {
		policy.ReplayJitterSeconds = 10
	}
	if policy.UncertainRetryDelaySeconds == 0 {
		policy.UncertainRetryDelaySeconds = 60
	}
	// Zero jitter in a complete operator snapshot is an explicit setting.
	if policy.AttemptBudget < 1 || policy.AttemptBudget > MaxTaskAttemptBudget ||
		policy.BackoffMillis < 1000 || policy.BackoffMillis > 60000 ||
		policy.ReconcileDelaySeconds < 1 || policy.ReconcileDelaySeconds > 3600 ||
		policy.ReplayInitialDelaySeconds < 30 || policy.ReplayInitialDelaySeconds > 3600 ||
		policy.ReplayMaxDelaySeconds < policy.ReplayInitialDelaySeconds || policy.ReplayMaxDelaySeconds > 86400 ||
		policy.ReplayJitterSeconds < 0 || policy.ReplayJitterSeconds > 60 ||
		policy.UncertainRetryDelaySeconds < 60 || policy.UncertainRetryDelaySeconds > 3600 {
		return TaskRetryPolicy{}, ErrTaskDispatchConflict
	}
	return policy, nil
}

func (dispatch TaskDispatch) ReconcileDelay() time.Duration {
	seconds := dispatch.ReconcileDelaySeconds
	if dispatch.RetryPolicyVersion == 0 {
		seconds = 15
	}
	return time.Duration(seconds) * time.Second
}

// TaskDispatchAttempt retains predecessor identity and receipt evidence when
// a verified successor is authorized. Supplier keys are private.
type TaskDispatchAttempt struct {
	TaskID                        int64  `gorm:"primaryKey;autoIncrement:false"`
	Ordinal                       int    `gorm:"primaryKey;autoIncrement:false"`
	SupplierKey                   string `json:"-" gorm:"type:varchar(128);not null"`
	AccountID                     string `json:"-" gorm:"type:varchar(191);not null"`
	RequestHash                   string `json:"-" gorm:"type:char(64);not null"`
	StartedAt                     int64
	ReplayUntil                   int64
	ReplayCount                   int
	NextReplayAt                  int64
	ReceiptEverSeen               bool
	ReceiptSeenAt                 int64
	ReceiptSeenEvidence           TaskBlob `json:"-"`
	SubmitStatusCode              int
	SubmitHeaders                 TaskBlob `json:"-"`
	SubmitHeadersTruncated        bool
	SubmitBody                    TaskBlob `json:"-"`
	SubmitBodySHA256              string   `json:"-" gorm:"type:char(64)"`
	SubmitBodyLength              int64
	SubmitBodyTruncated           bool
	SubmitBodyReadError           bool
	SubmitObservedAt              int64
	ReceiptKind                   string   `gorm:"type:varchar(24)"`
	ReceiptID                     string   `gorm:"type:varchar(191)"`
	EvidenceHash                  string   `gorm:"type:char(64)"`
	Evidence                      TaskBlob `json:"-"`
	SupplierNetQuota              int64
	SupplierBillingFinal          bool
	NewSubmissionSafe             bool
	TemporaryTechnicalCause       bool
	PublicErrorCode               string `gorm:"type:varchar(64)"`
	TaskLookupNotFound            bool
	FinalCustomerBillingReceiptID string `gorm:"type:varchar(191)"`
	UnknownObservedAt             int64
	ReconcileNextAt               int64
	OperatorAudit                 TaskBlob   `json:"-"`
	WatchState                    string     `gorm:"type:varchar(20);index"`
	WatchUpstreamID               string     `gorm:"type:varchar(191)"`
	WatchResultStatus             TaskStatus `gorm:"type:varchar(24)"`
	WatchPublicErrorCode          string     `gorm:"type:varchar(64)"`
	WatchFinalBillingReceiptID    string     `gorm:"type:varchar(191)"`
	WatchEvidenceHash             string     `gorm:"type:char(64)"`
	WatchObservedAt               int64
	WatchPollCount                int
	ResolvedAt                    int64
}

// Each delivery is separately auditable; the original response remains on
// TaskDispatchAttempt even when a same-key replay receives another response.
type TaskDispatchReplayObservation struct {
	TaskID           int64 `gorm:"primaryKey;autoIncrement:false"`
	Ordinal          int   `gorm:"primaryKey;autoIncrement:false"`
	ReplayNumber     int   `gorm:"primaryKey;autoIncrement:false"`
	StatusCode       int
	Headers          TaskBlob `json:"-"`
	HeadersTruncated bool
	Body             TaskBlob `json:"-"`
	BodySHA256       string   `json:"-" gorm:"type:char(64)"`
	BodyLength       int64
	BodyTruncated    bool
	BodyReadError    bool
	ObservedAt       int64
}

type TaskAtomicReplayPolicy struct {
	Kind                       string `json:"kind"`
	GuaranteedRetentionSeconds int64  `json:"guaranteedRetentionSeconds"`
}

const taskReplayWindowSeconds int64 = 29 * 24 * 3600

type TaskPreparedWire struct {
	Operation   string
	BaseURL     string
	Host        string
	Method      string
	Path        string
	ContentType string
	Headers     map[string]string
	// The host injects claim.SupplierKey under this header at send time. Its
	// value is absent from the frozen content hash so a verified successor can
	// retain identical request content with a new supplier key.
	SupplierKeyHeader string
	Body              []byte
	PreparedUsage     []byte
	PreparedState     []byte
	AccountID         string
	AtomicReplay      *TaskAtomicReplayPolicy
}

// PrepareTaskWire freezes the exact supplier request before SEND_STARTED.
// Credentials must remain in the frozen private channel/account snapshot,
// never in this row or the public Task response.
func PrepareTaskWire(ctx context.Context, claim TaskDispatchClaim, wire TaskPreparedWire) (string, error) {
	if wire.Operation == "" {
		wire.Operation = "submit"
	}
	if wire.AtomicReplay != nil && (wire.Operation != "submit" || wire.Method != http.MethodPost ||
		wire.AtomicReplay.Kind != "atomic_same_key" || wire.AtomicReplay.GuaranteedRetentionSeconds < 30*24*3600 ||
		wire.AtomicReplay.GuaranteedRetentionSeconds > 365*24*3600) {
		return "", ErrTaskDispatchConflict
	}
	atomicReplaySeconds := int64(0)
	atomicReplayGuaranteedSeconds := int64(0)
	if wire.AtomicReplay != nil {
		atomicReplayGuaranteedSeconds = wire.AtomicReplay.GuaranteedRetentionSeconds
		atomicReplaySeconds = min(taskReplayWindowSeconds, atomicReplayGuaranteedSeconds-3600)
	}
	if claim.TaskID <= 0 || claim.Owner == "" || wire.BaseURL == "" || wire.Method == "" || wire.Path == "" ||
		(wire.Operation != "submit" && wire.Operation != "adopt") ||
		wire.AccountID == "" || wire.SupplierKeyHeader == "" || wire.Host == "" || len(wire.BaseURL) > 2048 || len(wire.Host) > 255 ||
		len(wire.Method) > 12 || len(wire.Path) > 512 || len(wire.ContentType) > 255 ||
		len(wire.SupplierKeyHeader) > 128 || len(wire.Body) > MaxTaskRawInputBytes || len(wire.PreparedUsage) > 1<<20 || len(wire.PreparedState) > 1<<20 ||
		strings.ContainsFunc(wire.BaseURL+wire.Host+wire.Method+wire.Path+wire.ContentType+wire.SupplierKeyHeader, unicode.IsControl) ||
		strings.ContainsAny(wire.Host, " /\\") {
		return "", ErrTaskDispatchConflict
	}
	keyHeader := strings.ToLower(wire.SupplierKeyHeader)
	headers := make(map[string]string, len(wire.Headers))
	for name, value := range wire.Headers {
		canonical := strings.ToLower(name)
		switch canonical {
		case "content-length", "transfer-encoding", "connection", "proxy-connection", "trailer", "upgrade":
			return "", ErrTaskDispatchConflict
		}
		if canonical == "" || canonical == keyHeader || len(canonical) > 128 || len(value) > 4096 ||
			strings.ContainsFunc(name+value, unicode.IsControl) {
			return "", ErrTaskDispatchConflict
		}
		if _, exists := headers[canonical]; exists {
			return "", ErrTaskDispatchConflict
		}
		headers[canonical] = value
	}
	baseURL, err := url.Parse(wire.BaseURL)
	if err != nil || baseURL.Host == "" ||
		(headers["host"] != "" && headers["host"] != wire.Host) ||
		(headers["host"] == "" && wire.Host != baseURL.Host) {
		return "", ErrTaskDispatchConflict
	}
	if len(headers) > 64 {
		return "", ErrTaskDispatchConflict
	}
	headerJSON, err := common.Marshal(headers)
	if err != nil {
		return "", err
	}
	body := append([]byte(nil), wire.Body...)
	var intake TaskIntake
	if err := DB.WithContext(ctx).Where("task_id = ?", claim.TaskID).First(&intake).Error; err != nil {
		return "", err
	}
	if intake.AccountID != wire.AccountID || intake.SupplierBaseURL != wire.BaseURL ||
		len(intake.RoutingSnapshot) == 0 {
		return "", ErrTaskDispatchConflict
	}
	routeDigest := sha256.Sum256(intake.RoutingSnapshot)
	routeHash := hex.EncodeToString(routeDigest[:])
	hash := taskPreparedWireHash(wire, routeDigest, keyHeader, headers)
	err = DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var currentIntake TaskIntake
		if err := tx.Where("task_id = ?", claim.TaskID).First(&currentIntake).Error; err != nil {
			return err
		}
		if currentIntake.AccountID != wire.AccountID || currentIntake.SupplierBaseURL != wire.BaseURL ||
			len(currentIntake.RoutingSnapshot) == 0 ||
			sha256.Sum256(currentIntake.RoutingSnapshot) != routeDigest {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Phase != TaskDispatchReady || dispatch.Version != claim.Version || dispatch.Owner != claim.Owner ||
			dispatch.LeaseUntil <= time.Now().UnixMilli() {
			return ErrTaskDispatchConflict
		}
		if dispatch.RequestHash != "" {
			if dispatch.RequestHash == hash && dispatch.AccountID == wire.AccountID && dispatch.RoutingHash == routeHash && dispatch.WireOperation == wire.Operation {
				return nil
			}
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND request_hash = ?", claim.TaskID, claim.Version, "").
			Updates(map[string]any{"request_hash": hash, "routing_hash": routeHash, "stage_at": time.Now().Unix(),
				"wire_operation":                   wire.Operation,
				"atomic_replay_seconds":            atomicReplaySeconds,
				"atomic_replay_guaranteed_seconds": atomicReplayGuaranteedSeconds,
				"wire_base_url":                    wire.BaseURL, "wire_host": wire.Host, "wire_method": wire.Method, "wire_path": wire.Path,
				"wire_type": wire.ContentType, "wire_key_header": keyHeader, "wire_headers": TaskBlob(headerJSON),
				"wire_body": TaskBlob(body), "account_id": wire.AccountID,
				"prepared_usage": TaskBlob(wire.PreparedUsage), "prepared_state": TaskBlob(wire.PreparedState)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
	return hash, err
}

func taskPreparedWireHash(wire TaskPreparedWire, routeDigest [32]byte, keyHeader string, headers map[string]string) string {
	h := sha256.New()
	write := func(value []byte) {
		_, _ = fmt.Fprintf(h, "%d:", len(value))
		_, _ = h.Write(value)
	}
	write([]byte(wire.BaseURL))
	write([]byte(wire.Host))
	write([]byte(wire.AccountID))
	write([]byte(wire.Operation))
	if wire.AtomicReplay != nil {
		write([]byte(wire.AtomicReplay.Kind))
		write([]byte(fmt.Sprint(wire.AtomicReplay.GuaranteedRetentionSeconds)))
	}
	write(routeDigest[:])
	write([]byte(wire.Method))
	write([]byte(wire.Path))
	write([]byte(wire.ContentType))
	write([]byte(keyHeader))
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		write([]byte(name))
		write([]byte(headers[name]))
	}
	write(wire.Body)
	write(wire.PreparedUsage)
	write(wire.PreparedState)
	return hex.EncodeToString(h.Sum(nil))
}

type TaskDispatchClaim struct {
	TaskID         int64
	Version        int64
	Owner          string
	LeaseUntil     int64
	Attempt        int
	ReconcileDelay time.Duration
	SupplierKey    string `json:"-"`
	Phase          string
}

// ListDueTaskDispatchIDs uses a cursor so a blocked early row cannot hide
// later tasks. Each candidate is still checked by ClaimTaskDispatch.
func ListDueTaskDispatchIDs(ctx context.Context, afterTaskID int64, limit int) ([]int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var ids []int64
	err := DB.WithContext(ctx).Model(&TaskDispatch{}).Where("task_id > ? AND lease_until <= ? AND next_attempt_at <= ?", afterTaskID,
		time.Now().UnixMilli(), time.Now().UnixMilli()).Where("phase IN ?", []string{
		TaskDispatchReady, TaskDispatchSendStarted, TaskDispatchAdopting, TaskDispatchUnknown, TaskDispatchAccepted, TaskDispatchRejected,
	}).Where("EXISTS (SELECT 1 FROM tasks WHERE tasks.id = task_dispatches.task_id AND tasks.status NOT IN ?)",
		[]TaskStatus{TaskStatusSuccess, TaskStatusFailure}).Order("task_id asc").Limit(limit).Pluck("task_id", &ids).Error
	return ids, err
}

func HasDueTaskDispatch(ctx context.Context) (bool, error) {
	var count int64
	err := DB.WithContext(ctx).Model(&TaskDispatch{}).Where("lease_until <= ? AND next_attempt_at <= ?", time.Now().UnixMilli(), time.Now().UnixMilli()).
		Where("phase IN ?", []string{TaskDispatchReady, TaskDispatchSendStarted, TaskDispatchAdopting, TaskDispatchUnknown, TaskDispatchAccepted, TaskDispatchRejected}).
		Where("EXISTS (SELECT 1 FROM tasks WHERE tasks.id = task_dispatches.task_id AND tasks.status NOT IN ?)",
			[]TaskStatus{TaskStatusSuccess, TaskStatusFailure}).Limit(1).Count(&count).Error
	return count > 0, err
}

// DeferTaskDispatch releases a claim after a bounded observation failure.
// It never changes the send phase or grants another supplier key.
func DeferTaskDispatch(ctx context.Context, claim TaskDispatchClaim, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	if delay > time.Hour {
		delay = time.Hour
	}
	delay += time.Duration((claim.TaskID*37+claim.Version*791)%1000) * time.Millisecond
	due := time.Now().Add(delay).UnixMilli()
	result := DB.WithContext(ctx).Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND owner = ? AND lease_until > ?",
		claim.TaskID, claim.Version, claim.Owner, time.Now().UnixMilli()).Updates(map[string]any{
		"version": claim.Version + 1, "owner": "", "lease_until": int64(0),
		"next_attempt_at": gorm.Expr("CASE WHEN next_attempt_at > ? THEN next_attempt_at ELSE ? END", due, due),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

// BlockTaskDispatch preserves the original command and reserve for operator
// repair. This state has no scheduler authority and is unrelated to a verified
// unpaid predecessor eligible for a new supplier key.
func BlockTaskDispatch(ctx context.Context, claim TaskDispatchClaim, reason string, evidence ...[]byte) error {
	contextBlock := reason == "frozen_context_invalid" && (claim.Phase == TaskDispatchReady ||
		claim.Phase == TaskDispatchSendStarted || claim.Phase == TaskDispatchUnknown ||
		claim.Phase == TaskDispatchAdopting || claim.Phase == TaskDispatchAccepted || claim.Phase == TaskDispatchRejected)
	if !contextBlock && !(reason == "frozen_input_mismatch" && claim.Phase == TaskDispatchReady) &&
		!(reason == "adoption_authorization_failed" && claim.Phase == TaskDispatchAdopting) &&
		!(reason == "pre_send_receipt_seen" && claim.Phase == TaskDispatchReady && len(evidence) == 1 &&
			len(evidence[0]) > 0 && len(evidence[0]) <= 64<<10) {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() ||
			dispatch.Phase != claim.Phase || dispatch.Attempt != claim.Attempt {
			return ErrTaskDispatchConflict
		}
		if reason == "frozen_input_mismatch" && dispatch.RequestHash != "" ||
			reason == "adoption_authorization_failed" && (dispatch.WireOperation != "adopt" || dispatch.RequestHash == "") {
			return ErrTaskDispatchConflict
		}
		if reason == "pre_send_receipt_seen" {
			var intake TaskIntake
			if err := tx.Where("task_id = ?", claim.TaskID).Take(&intake).Error; err != nil {
				return err
			}
			var observed TaskReceiptEvidence
			if dispatch.RequestHash == "" || dispatch.AtomicReplaySeconds == 0 ||
				common.Unmarshal(evidence[0], &observed) != nil || !validTaskReceiptSeenEvidence(observed, dispatch, intake) {
				return ErrTaskDispatchConflict
			}
		}
		if reason == "adoption_authorization_failed" {
			var observation TaskHTTPObservation
			if err := tx.Where("task_id = ? AND attempt = ? AND kind = ? AND status_code IN ?", claim.TaskID, claim.Attempt,
				"adopt", []int{http.StatusUnauthorized, http.StatusForbidden}).Take(&observation).Error; err != nil {
				return err
			}
		}
		updates := map[string]any{"version": claim.Version + 1, "phase": TaskDispatchBlocked, "blocked_from": claim.Phase,
			"blocked_reason": reason, "blocked_task_status": task.Status, "blocked_task_progress": task.Progress,
			"stage_at": time.Now().Unix(), "owner": "", "lease_until": int64(0)}
		if reason == "pre_send_receipt_seen" {
			updates["pre_send_receipt_evidence"] = TaskBlob(evidence[0])
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ? AND owner = ?", claim.TaskID,
			claim.Version, claim.Phase, claim.Owner).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return tx.Model(&Task{}).Where("id = ?", claim.TaskID).Updates(map[string]any{
			"status": TaskStatusUnknown, "progress": "Blocked", "updated_at": time.Now().Unix(),
		}).Error
	})
}

// ResumeBlockedTaskDispatch restores only the same phase, command and key.
// The root actor and reason are bounded audit facts, never supplier evidence.
func ResumeBlockedTaskDispatch(ctx context.Context, taskID, expectedVersion int64, reason string, actor int) error {
	if taskID <= 0 || expectedVersion <= 0 || actor <= 0 || len(reason) < 3 || len(reason) > 64 ||
		strings.Trim(reason, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" || reason[0] == '_' {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status != TaskStatusUnknown {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != expectedVersion || dispatch.Phase != TaskDispatchBlocked || dispatch.Owner != "" ||
			(dispatch.BlockedReason == "frozen_context_invalid" && dispatch.BlockedFrom != TaskDispatchReady &&
				dispatch.BlockedFrom != TaskDispatchSendStarted && dispatch.BlockedFrom != TaskDispatchUnknown &&
				dispatch.BlockedFrom != TaskDispatchAdopting && dispatch.BlockedFrom != TaskDispatchAccepted &&
				dispatch.BlockedFrom != TaskDispatchRejected) ||
			(dispatch.BlockedReason != "frozen_context_invalid" && dispatch.BlockedFrom != TaskDispatchReady && dispatch.BlockedFrom != TaskDispatchAdopting) {
			return ErrTaskDispatchConflict
		}
		var observation TaskHTTPObservation
		err = tx.Where("task_id = ?", taskID).Take(&observation).Error
		if dispatch.BlockedReason == "frozen_context_invalid" {
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		} else if dispatch.BlockedFrom == TaskDispatchReady {
			if err == nil || !errors.Is(err, gorm.ErrRecordNotFound) || dispatch.RequestHash != "" {
				return ErrTaskDispatchConflict
			}
		} else {
			if err != nil || observation.Kind != "adopt" || observation.Attempt != dispatch.Attempt ||
				(observation.StatusCode != http.StatusUnauthorized && observation.StatusCode != http.StatusForbidden) ||
				dispatch.WireOperation != "adopt" || dispatch.RequestHash == "" {
				return ErrTaskDispatchConflict
			}
			deleted := tx.Where("task_id = ? AND attempt = ? AND kind = ? AND status_code IN ?", taskID,
				dispatch.Attempt, "adopt", []int{http.StatusUnauthorized, http.StatusForbidden}).Delete(&TaskHTTPObservation{})
			if deleted.Error != nil {
				return deleted.Error
			}
			if deleted.RowsAffected != 1 {
				return ErrTaskDispatchConflict
			}
		}
		if dispatch.BlockedTaskStatus == "" || dispatch.BlockedTaskStatus == TaskStatusSuccess || dispatch.BlockedTaskStatus == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ?", taskID, expectedVersion, TaskDispatchBlocked).
			Updates(map[string]any{"version": expectedVersion + 1, "phase": dispatch.BlockedFrom, "blocked_from": "",
				"blocked_reason": "", "blocked_task_status": "", "blocked_task_progress": "",
				"resume_reason": fmt.Sprintf("root:%d %s", actor, reason), "stage_at": time.Now().Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return tx.Model(&Task{}).Where("id = ?", taskID).Updates(map[string]any{
			"status": dispatch.BlockedTaskStatus, "progress": dispatch.BlockedTaskProgress, "updated_at": time.Now().Unix(),
		}).Error
	})
}

type BlockedTaskDispatchStatus struct {
	TaskID         string `json:"task_id"`
	Version        int64  `json:"version"`
	Phase          string `json:"phase"`
	BlockedFrom    string `json:"blocked_from"`
	BlockedReason  string `json:"blocked_reason"`
	Reserved       bool   `json:"reserved"`
	Observation    string `json:"observation"`
	HTTPStatusCode int    `json:"http_status_code,omitempty"`
}

func GetBlockedTaskDispatchStatus(ctx context.Context, publicID string) (BlockedTaskDispatchStatus, error) {
	var view BlockedTaskDispatchStatus
	if len(publicID) < 6 || len(publicID) > 128 || !strings.HasPrefix(publicID, "task_") {
		return view, ErrTaskDispatchConflict
	}
	var task Task
	if err := DB.WithContext(ctx).Select("id", "task_id").Where("task_id = ?", publicID).Take(&task).Error; err != nil {
		return view, err
	}
	var dispatch TaskDispatch
	if err := DB.WithContext(ctx).Select("task_id", "version", "phase", "blocked_from", "blocked_reason").
		Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
		return view, err
	}
	view = BlockedTaskDispatchStatus{TaskID: task.TaskID, Version: dispatch.Version, Phase: dispatch.Phase,
		BlockedFrom: dispatch.BlockedFrom, BlockedReason: dispatch.BlockedReason}
	var count int64
	if err := DB.WithContext(ctx).Model(&TaskFinance{}).Where("task_id = ? AND state = ?", task.ID, "reserved").Count(&count).Error; err != nil {
		return view, err
	}
	view.Reserved = count == 1
	var observation TaskHTTPObservation
	if err := DB.WithContext(ctx).Select("kind", "status_code").Where("task_id = ?", task.ID).Take(&observation).Error; err == nil {
		view.Observation, view.HTTPStatusCode = observation.Kind, observation.StatusCode
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return view, err
	}
	return view, nil
}

// AcceptTaskDispatch publishes the one supplier identity without changing
// the public ID or using legacy task terminal CAS.
func AcceptTaskDispatch(ctx context.Context, claim TaskDispatchClaim, upstreamID string, data, state []byte, publishData, clearSubmitObservation bool) error {
	if (claim.Phase != TaskDispatchSendStarted && claim.Phase != TaskDispatchAdopting) || upstreamID == "" || len(upstreamID) > 191 ||
		len(data) > MaxPendingTaskOutcomeBytes || len(state) > 1<<20 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() ||
			dispatch.Phase != claim.Phase || dispatch.Attempt != claim.Attempt ||
			task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		observationKind := "submit"
		if claim.Phase == TaskDispatchAdopting {
			observationKind = "adopt"
		}
		var observation TaskHTTPObservation
		if err := tx.Where("task_id = ? AND attempt = ? AND kind = ?", claim.TaskID, claim.Attempt, observationKind).
			First(&observation).Error; err != nil {
			return err
		}
		if claim.Phase == TaskDispatchSendStarted {
			attemptUpdates := map[string]any{"submit_status_code": observation.StatusCode,
				"submit_headers": TaskBlob(observation.Headers), "submit_body": TaskBlob(observation.Body),
				"submit_observed_at": observation.ObservedAt}
			if dispatch.UncertainReplacementUsed {
				attemptUpdates["watch_state"] = "pending"
				attemptUpdates["watch_upstream_id"] = upstreamID
				attemptUpdates["reconcile_next_at"] = time.Now().Add(dispatch.ReconcileDelay()).UnixMilli()
			}
			result := tx.Model(&TaskDispatchAttempt{}).
				Where("task_id = ? AND ordinal = ? AND submit_observed_at = ?", claim.TaskID, claim.Attempt, 0).
				Updates(attemptUpdates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskDispatchConflict
			}
		}
		task.PrivateData.UpstreamTaskID = upstreamID
		task.PrivateData.PluginState = append(task.PrivateData.PluginState[:0:0], state...)
		var taskData any
		if publishData && len(data) > 0 {
			taskData = string(data)
		}
		if err := tx.Model(&Task{}).Where("id = ?", claim.TaskID).Updates(map[string]any{
			"status": TaskStatusSubmitted, "private_data": task.PrivateData, "data": taskData,
			"updated_at": time.Now().Unix(),
		}).Error; err != nil {
			return err
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ?", claim.TaskID, claim.Version).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchAccepted, "stage_at": time.Now().Unix(),
				"upstream_id": upstreamID, "owner": "", "lease_until": int64(0)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		if clearSubmitObservation {
			result = tx.Where("task_id = ? AND attempt = ? AND kind = ?", claim.TaskID, claim.Attempt, observationKind).
				Delete(&TaskHTTPObservation{})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskDispatchConflict
			}
		}
		return nil
	})
}

func UpdateAcceptedTaskProgress(ctx context.Context, claim TaskDispatchClaim, status TaskStatus, progress string, state []byte) error {
	if status != TaskStatusSubmitted && status != TaskStatusQueued && status != TaskStatusInProgress ||
		len(progress) > 20 || len(state) > 1<<20 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Phase != TaskDispatchAccepted || dispatch.Version != claim.Version || dispatch.Owner != claim.Owner ||
			dispatch.LeaseUntil <= time.Now().UnixMilli() || task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		if len(state) > 0 {
			task.PrivateData.PluginState = append(task.PrivateData.PluginState[:0:0], state...)
		}
		updates := map[string]any{"status": status, "private_data": task.PrivateData, "updated_at": time.Now().Unix()}
		if progress != "" {
			updates["progress"] = progress
		}
		if status == TaskStatusInProgress && task.StartTime == 0 {
			updates["start_time"] = time.Now().Unix()
		}
		return tx.Model(&Task{}).Where("id = ?", claim.TaskID).Updates(updates).Error
	})
}

func newTaskSupplierKey() (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

// ClaimTaskDispatch grants a bounded command lease. Expiry of an unresolved
// send first grants receipt reconciliation; any exact same-key delivery needs
// its own frozen policy and StartTaskReplay CAS.
func ClaimTaskDispatch(ctx context.Context, taskID int64, owner string, lease time.Duration) (*TaskDispatchClaim, error) {
	if taskID <= 0 || owner == "" || len(owner) > 128 || lease <= 0 || lease > 2*time.Minute {
		return nil, ErrTaskDispatchConflict
	}
	now := time.Now().UnixMilli()
	deadline := now + lease.Milliseconds()
	var claim TaskDispatchClaim
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.LeaseUntil > now || dispatch.NextAttemptAt > now ||
			(dispatch.Phase != TaskDispatchReady && dispatch.Phase != TaskDispatchSendStarted && dispatch.Phase != TaskDispatchAdopting && dispatch.Phase != TaskDispatchUnknown && dispatch.Phase != TaskDispatchAccepted && dispatch.Phase != TaskDispatchRejected) {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND lease_until <= ?", taskID, dispatch.Version, now).
			Updates(map[string]any{"version": dispatch.Version + 1, "owner": owner, "lease_until": deadline})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		claim = TaskDispatchClaim{TaskID: taskID, Version: dispatch.Version + 1, Owner: owner, LeaseUntil: deadline,
			Attempt: dispatch.Attempt, ReconcileDelay: dispatch.ReconcileDelay(), SupplierKey: dispatch.SupplierKey, Phase: dispatch.Phase}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &claim, nil
}

func RenewTaskDispatch(ctx context.Context, claim TaskDispatchClaim, lease time.Duration) (*TaskDispatchClaim, error) {
	if claim.TaskID <= 0 || claim.Owner == "" || lease <= 0 || lease > 2*time.Minute {
		return nil, ErrTaskDispatchConflict
	}
	now := time.Now().UnixMilli()
	deadline := now + lease.Milliseconds()
	result := DB.WithContext(ctx).Model(&TaskDispatch{}).
		Where("task_id = ? AND version = ? AND owner = ? AND lease_until > ?", claim.TaskID, claim.Version, claim.Owner, now).
		Updates(map[string]any{"version": claim.Version + 1, "lease_until": deadline})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, ErrTaskDispatchConflict
	}
	claim.Version++
	claim.LeaseUntil = deadline
	return &claim, nil
}

// StartTaskSend is the durable paid-send barrier. The caller must use the
// persisted supplier key and exact request bytes for provider-side dedup.
func StartTaskSend(ctx context.Context, claim TaskDispatchClaim, requestHash, accountID string) (*TaskDispatchClaim, error) {
	if requestHash == "" || len(requestHash) != 64 || accountID == "" {
		return nil, ErrTaskDispatchConflict
	}
	now := time.Now().UnixMilli()
	var next TaskDispatchClaim
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var intake TaskIntake
		if err := tx.Where("task_id = ?", claim.TaskID).First(&intake).Error; err != nil {
			return err
		}
		if intake.AccountID != accountID {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= now || dispatch.Phase != TaskDispatchReady ||
			dispatch.Attempt >= dispatch.AttemptBudget {
			return ErrTaskDispatchConflict
		}
		if dispatch.WireOperation != "submit" || dispatch.RequestHash == "" || dispatch.RequestHash != requestHash || dispatch.AccountID != accountID ||
			dispatch.RoutingHash == "" || dispatch.WireBaseURL == "" || dispatch.WireKeyHeader == "" {
			return ErrTaskDispatchConflict
		}
		if dispatch.SupplierKey == "" {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ?", claim.TaskID, claim.Version, TaskDispatchReady).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchSendStarted, "stage_at": time.Now().Unix(), "attempt": dispatch.Attempt + 1,
				"supplier_key": dispatch.SupplierKey, "request_hash": requestHash, "account_id": accountID})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		if err := tx.Create(&TaskDispatchAttempt{TaskID: claim.TaskID, Ordinal: dispatch.Attempt + 1,
			SupplierKey: dispatch.SupplierKey, AccountID: accountID, RequestHash: requestHash, StartedAt: now,
			WatchState: func() string {
				if dispatch.UncertainReplacementUsed {
					return "pending"
				}
				return ""
			}(),
			ReconcileNextAt: func() int64 {
				if dispatch.UncertainReplacementUsed {
					return now + dispatch.ReconcileDelay().Milliseconds()
				}
				return 0
			}(),
			ReplayUntil: func() int64 {
				if dispatch.AtomicReplaySeconds > 0 {
					return now + dispatch.AtomicReplaySeconds*1000
				}
				return 0
			}()}).Error; err != nil {
			return err
		}
		next = TaskDispatchClaim{TaskID: claim.TaskID, Version: claim.Version + 1, Owner: claim.Owner, LeaseUntil: claim.LeaseUntil,
			Attempt: dispatch.Attempt + 1, ReconcileDelay: claim.ReconcileDelay, SupplierKey: dispatch.SupplierKey, Phase: TaskDispatchSendStarted}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &next, nil
}

// RecordTaskReceiptSeen fences replay before any supplier payload parsing.
// The first bounded HTTP evidence is retained even if later parsing fails.
func RecordTaskReceiptSeen(ctx context.Context, claim TaskDispatchClaim, evidence []byte) error {
	if len(evidence) == 0 || len(evidence) > 64<<10 {
		return ErrTaskDispatchConflict
	}
	var observed TaskReceiptEvidence
	if err := common.Unmarshal(evidence, &observed); err != nil || observed.Method != http.MethodGet ||
		observed.StatusCode != http.StatusOK || observed.ObservedAt <= 0 ||
		len(observed.Body) > 32<<10 || len(observed.Headers) > 16<<10 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil || hasOutcome {
			if err != nil {
				return err
			}
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() ||
			dispatch.Attempt != claim.Attempt || dispatch.Phase != claim.Phase ||
			(claim.Phase != TaskDispatchSendStarted && claim.Phase != TaskDispatchUnknown) {
			return ErrTaskDispatchConflict
		}
		var intake TaskIntake
		if err := tx.Where("task_id = ?", claim.TaskID).Take(&intake).Error; err != nil {
			return err
		}
		var attempt TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&attempt).Error; err != nil {
			return err
		}
		if attempt.SupplierKey != dispatch.SupplierKey || attempt.AccountID != dispatch.AccountID ||
			attempt.RequestHash != dispatch.RequestHash {
			return ErrTaskDispatchConflict
		}
		if !validTaskReceiptSeenEvidence(observed, dispatch, intake) {
			return ErrTaskDispatchConflict
		}
		if attempt.ReceiptEverSeen {
			return nil
		}
		result := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND receipt_ever_seen = ?",
			claim.TaskID, claim.Attempt, false).Updates(map[string]any{"receipt_ever_seen": true,
			"receipt_seen_at": time.Now().Unix(), "receipt_seen_evidence": TaskBlob(evidence)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}

// StartTaskReplay authorizes one exact same-key delivery. The ordinal,
// StartedAt, reserve and original supplier key are immutable.
func StartTaskReplay(ctx context.Context, claim TaskDispatchClaim) (*TaskDispatchClaim, bool, error) {
	if claim.Phase != TaskDispatchSendStarted && claim.Phase != TaskDispatchUnknown {
		return nil, false, ErrTaskDispatchConflict
	}
	now := time.Now().UnixMilli()
	var next TaskDispatchClaim
	allowed := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var intake TaskIntake
		if err := tx.Where("task_id = ?", claim.TaskID).First(&intake).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= now ||
			dispatch.Phase != claim.Phase || dispatch.Attempt != claim.Attempt {
			return ErrTaskDispatchConflict
		}
		var attempt TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&attempt).Error; err != nil {
			return err
		}
		var observation TaskHTTPObservation
		obsErr := tx.Where("task_id = ?", claim.TaskID).Take(&observation).Error
		if obsErr == nil {
			return ErrTaskDispatchConflict
		}
		if !errors.Is(obsErr, gorm.ErrRecordNotFound) {
			return obsErr
		}
		routeDigest := sha256.Sum256(intake.RoutingSnapshot)
		if dispatch.AtomicReplaySeconds == 0 {
			return nil
		}
		if attempt.StartedAt <= 0 || attempt.ReplayUntil != attempt.StartedAt+dispatch.AtomicReplaySeconds*1000 ||
			attempt.ReplayCount < 0 || attempt.ReplayCount >= 10_000 ||
			attempt.SupplierKey != dispatch.SupplierKey || attempt.SupplierKey != claim.SupplierKey ||
			attempt.AccountID != dispatch.AccountID || attempt.AccountID != intake.AccountID ||
			attempt.RequestHash != dispatch.RequestHash || dispatch.RoutingHash != hex.EncodeToString(routeDigest[:]) ||
			dispatch.WireBaseURL != intake.SupplierBaseURL || dispatch.WireOperation != "submit" ||
			dispatch.WireMethod != http.MethodPost || dispatch.WirePath == "" || dispatch.WireKeyHeader == "" {
			return ErrTaskFrozenWireInvalid
		}
		initial, maximum, jitter := dispatch.ReplayInitialDelaySeconds, dispatch.ReplayMaxDelaySeconds, dispatch.ReplayJitterSeconds
		if dispatch.RetryPolicyVersion == 0 {
			initial, maximum, jitter = 60, 3840, 10
		}
		if initial < 30 || initial > 3600 || maximum < initial || maximum > 86400 || jitter < 0 || jitter > 60 {
			return ErrTaskFrozenWireInvalid
		}
		if now+60_000 >= attempt.ReplayUntil || now < dispatch.NextAttemptAt ||
			now < max(attempt.NextReplayAt, attempt.StartedAt+initial*1000) ||
			attempt.ReceiptEverSeen || attempt.ReceiptID != "" {
			return nil
		}
		var headers map[string]string
		if err := common.Unmarshal(dispatch.WireHeaders, &headers); err != nil {
			return ErrTaskFrozenWireInvalid
		}
		policy := &TaskAtomicReplayPolicy{Kind: "atomic_same_key", GuaranteedRetentionSeconds: dispatch.AtomicReplayGuaranteedSeconds}
		if dispatch.AtomicReplayGuaranteedSeconds < 30*24*3600 ||
			taskPreparedWireHash(TaskPreparedWire{Operation: dispatch.WireOperation, BaseURL: dispatch.WireBaseURL,
				Host: dispatch.WireHost, Method: dispatch.WireMethod, Path: dispatch.WirePath,
				ContentType: dispatch.WireType, Headers: headers, SupplierKeyHeader: dispatch.WireKeyHeader,
				Body: dispatch.WireBody, PreparedUsage: dispatch.PreparedUsage, PreparedState: dispatch.PreparedState,
				AccountID: dispatch.AccountID, AtomicReplay: policy}, routeDigest, dispatch.WireKeyHeader, headers) != dispatch.RequestHash {
			return ErrTaskFrozenWireInvalid
		}
		count := attempt.ReplayCount + 1
		delay := min(initial<<min(count-1, 16), maximum) * 1000
		if jitter > 0 {
			delay += (claim.TaskID*37 + claim.Version*791) % (jitter * 1000)
		}
		result := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND replay_count = ?",
			claim.TaskID, claim.Attempt, attempt.ReplayCount).Updates(map[string]any{
			"replay_count": count, "next_replay_at": now + delay})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		result = tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ?", claim.TaskID, claim.Version, claim.Phase).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchSendStarted, "stage_at": time.Now().Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		next = TaskDispatchClaim{TaskID: claim.TaskID, Version: claim.Version + 1, Owner: claim.Owner,
			LeaseUntil: claim.LeaseUntil, Attempt: claim.Attempt, ReconcileDelay: claim.ReconcileDelay,
			SupplierKey: claim.SupplierKey, Phase: TaskDispatchSendStarted}
		allowed = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &next, allowed, nil
}

// StartTaskAdoption freezes a read of an existing original. It creates no
// supplier attempt and can never enter the paid-send receipt/successor path.
func StartTaskAdoption(ctx context.Context, claim TaskDispatchClaim, requestHash, accountID string) (*TaskDispatchClaim, error) {
	if len(requestHash) != 64 || accountID == "" {
		return nil, ErrTaskDispatchConflict
	}
	now := time.Now().UnixMilli()
	var next TaskDispatchClaim
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		var intake TaskIntake
		if err := tx.Where("task_id = ?", claim.TaskID).First(&intake).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= now ||
			dispatch.Phase != TaskDispatchReady || dispatch.WireOperation != "adopt" ||
			dispatch.RequestHash != requestHash || dispatch.AccountID != accountID || intake.AccountID != accountID ||
			dispatch.RoutingHash == "" || dispatch.WireBaseURL == "" || dispatch.WireKeyHeader == "" {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ?", claim.TaskID, claim.Version, TaskDispatchReady).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchAdopting, "stage_at": time.Now().Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		next = TaskDispatchClaim{TaskID: claim.TaskID, Version: claim.Version + 1, Owner: claim.Owner,
			LeaseUntil: claim.LeaseUntil, Attempt: dispatch.Attempt, ReconcileDelay: claim.ReconcileDelay, Phase: TaskDispatchAdopting}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &next, nil
}

type TaskReceipt struct {
	Evidence []byte
}

// TaskReceiptEvidence is private observed HTTP material plus host request
// binding. Binding fields are not claims made by the supplier.
type TaskReceiptEvidence struct {
	Method             string            `json:"method"`
	URL                string            `json:"url"`
	RequestHost        string            `json:"requestHost"`
	RequestHeaders     map[string]string `json:"requestHeaders"`
	AccountID          string            `json:"accountId"`
	SupplierKey        string            `json:"supplierKey"`
	RequestHash        string            `json:"requestHash"`
	RequestBodyHash    string            `json:"requestBodyHash"`
	StatusCode         int               `json:"statusCode"`
	Headers            TaskBlob          `json:"headers"`
	Body               TaskBlob          `json:"body"`
	ResponseIncomplete bool              `json:"responseIncomplete,omitempty"`
	ObservedAt         int64             `json:"observedAt"`
}

func validTaskReceiptSeenEvidence(observed TaskReceiptEvidence, dispatch TaskDispatch, intake TaskIntake) bool {
	if observed.Method != http.MethodGet || observed.StatusCode != http.StatusOK || observed.ObservedAt <= 0 ||
		len(observed.Body) > 32<<10 || len(observed.Headers) > 16<<10 ||
		observed.AccountID != intake.AccountID || observed.AccountID != dispatch.AccountID ||
		observed.SupplierKey != dispatch.SupplierKey || observed.RequestHash != dispatch.RequestHash {
		return false
	}
	bodyHash := sha256.Sum256(dispatch.WireBody)
	parsedURL, urlErr := url.Parse(observed.URL)
	baseURL, baseErr := url.Parse(intake.SupplierBaseURL)
	if urlErr != nil || baseErr != nil || parsedURL.Scheme != baseURL.Scheme || parsedURL.Host != baseURL.Host ||
		observed.RequestBodyHash != hex.EncodeToString(bodyHash[:]) ||
		observed.RequestHost != parsedURL.Host || observed.RequestHeaders["Authorization"] == "" ||
		!taskReceiptURLBindsKey(parsedURL, dispatch.SupplierKey, observed.RequestHeaders) {
		return false
	}
	if dispatch.AtomicReplaySeconds == 0 {
		return true
	}
	if dispatch.WireBaseURL != intake.SupplierBaseURL || dispatch.WireOperation != "submit" {
		return false
	}
	var wireHeaders map[string]string
	if common.Unmarshal(dispatch.WireHeaders, &wireHeaders) != nil {
		return false
	}
	return wireHeaders["Authorization"] != "" &&
		observed.RequestHeaders["Authorization"] == wireHeaders["Authorization"]
}

func taskReceiptURLBindsKey(parsedURL *url.URL, supplierKey string, headers map[string]string) bool {
	for _, segment := range strings.Split(parsedURL.EscapedPath(), "/") {
		decoded, err := url.PathUnescape(segment)
		if err == nil && decoded == supplierKey {
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

// TaskReceiptVerifier is a trusted host adapter, not a plugin boolean. It
// authenticates an exact supplier receipt outside the primary-DB transaction.
type TaskReceiptVerifier interface {
	VerifyTaskReceipt(context.Context, TaskDispatchAttempt, TaskReceipt) (TaskVerifiedReceipt, error)
}

type TaskVerifiedReceipt struct {
	Kind                          string // accepted, rejected, not_accepted, unknown
	ReceiptID                     string
	EvidenceHash                  string
	AccountID                     string
	RequestHash                   string
	SupplierKey                   string `json:"-"`
	UpstreamID                    string
	WatchUpstreamID               string
	TaskData                      []byte `json:"-"`
	PluginState                   []byte `json:"-"`
	SupplierNetQuota              int64
	SupplierBillingFinal          bool
	NewSubmissionSafe             bool
	TemporaryTechnicalCause       bool
	PublicErrorCode               string
	TaskLookupNotFound            bool
	ObservedAt                    int64
	FinalCustomerBillingReceiptID string
}

// ReconcileTaskReceipt never infers non-acceptance from timeout, 404, or a
// refund. Only exact-bound explicit supplier evidence can permit a successor.
func ReconcileTaskReceipt(ctx context.Context, claim TaskDispatchClaim, receipt TaskReceipt, verifier TaskReceiptVerifier) error {
	if claim.TaskID <= 0 || claim.Owner == "" || verifier == nil || len(receipt.Evidence) == 0 || len(receipt.Evidence) > 64<<10 {
		return ErrTaskDispatchConflict
	}
	evidence := append([]byte(nil), receipt.Evidence...)
	evidenceDigest := sha256.Sum256(evidence)
	var original TaskDispatchAttempt
	if err := DB.WithContext(ctx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&original).Error; err != nil {
		return err
	}
	verified, err := verifier.VerifyTaskReceipt(ctx, original, TaskReceipt{Evidence: append([]byte(nil), evidence...)})
	if err != nil {
		return err
	}
	if verified.EvidenceHash != hex.EncodeToString(evidenceDigest[:]) {
		return ErrTaskDispatchConflict
	}
	if verified.ReceiptID == "" || len(verified.ReceiptID) > 191 || len(verified.EvidenceHash) != 64 ||
		verified.AccountID != original.AccountID || verified.RequestHash != original.RequestHash ||
		verified.SupplierKey != original.SupplierKey || verified.SupplierNetQuota < 0 ||
		len(verified.FinalCustomerBillingReceiptID) > 191 || len(verified.WatchUpstreamID) > 191 ||
		len(verified.TaskData) > 1<<20 || len(verified.PluginState) > 1<<20 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() ||
			(dispatch.Phase != TaskDispatchSendStarted && dispatch.Phase != TaskDispatchUnknown) ||
			verified.AccountID != dispatch.AccountID || verified.RequestHash != dispatch.RequestHash ||
			verified.SupplierKey != dispatch.SupplierKey || dispatch.Attempt != claim.Attempt {
			return ErrTaskDispatchConflict
		}
		var attempt TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&attempt).Error; err != nil {
			return err
		}
		if (attempt.ReceiptID != "" && (attempt.ReceiptKind != "unknown" || attempt.ReceiptID != verified.ReceiptID)) ||
			attempt.SupplierKey != verified.SupplierKey || attempt.RequestHash != verified.RequestHash ||
			attempt.AccountID != verified.AccountID ||
			(attempt.ReceiptKind == "unknown" && verified.ObservedAt < attempt.UnknownObservedAt) ||
			(attempt.WatchUpstreamID != "" && verified.WatchUpstreamID != "" &&
				attempt.WatchUpstreamID != verified.WatchUpstreamID) {
			return ErrTaskDispatchConflict
		}
		updates := map[string]any{"version": claim.Version + 1, "owner": "", "lease_until": int64(0), "stage_at": time.Now().Unix()}
		switch verified.Kind {
		case "accepted":
			if verified.UpstreamID == "" {
				return ErrTaskDispatchConflict
			}
			updates["phase"] = TaskDispatchAccepted
			updates["next_attempt_at"] = max(dispatch.NextAttemptAt, time.Now().UnixMilli())
			updates["upstream_id"] = verified.UpstreamID
			task.PrivateData.UpstreamTaskID = verified.UpstreamID
			if len(verified.PluginState) > 0 {
				task.PrivateData.PluginState = append(task.PrivateData.PluginState[:0:0], verified.PluginState...)
			}
			var taskData any
			if len(verified.TaskData) > 0 {
				taskData = string(verified.TaskData)
			}
			if err := tx.Model(&Task{}).Where("id = ? AND status NOT IN ?", claim.TaskID,
				[]TaskStatus{TaskStatusSuccess, TaskStatusFailure}).Updates(map[string]any{
				"status": TaskStatusSubmitted, "private_data": task.PrivateData,
				"data":       taskData,
				"updated_at": time.Now().Unix(),
			}).Error; err != nil {
				return err
			}
		case "rejected":
			if !verified.SupplierBillingFinal || verified.SupplierNetQuota != 0 || verified.PublicErrorCode == "" ||
				len(verified.PublicErrorCode) > 64 || strings.Trim(verified.PublicErrorCode, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
				return ErrTaskDispatchConflict
			}
			updates["phase"] = TaskDispatchRejected
			updates["next_attempt_at"] = int64(0)
		case "not_accepted":
			if !verified.NewSubmissionSafe || !verified.SupplierBillingFinal || verified.SupplierNetQuota != 0 ||
				!verified.TemporaryTechnicalCause {
				return ErrTaskDispatchConflict
			}
			if dispatch.UncertainReplacementUsed {
				updates["phase"] = TaskDispatchParked
				if err := tx.Model(&Task{}).Where("id = ?", claim.TaskID).Updates(map[string]any{
					"status": TaskStatusUnknown, "fail_reason": "Task requires reconciliation",
					"updated_at": time.Now().Unix(),
				}).Error; err != nil {
					return err
				}
			} else if dispatch.Attempt >= dispatch.AttemptBudget {
				updates["phase"] = TaskDispatchRejected
				updates["next_attempt_at"] = int64(0)
			} else {
				key, err := newTaskSupplierKey()
				if err != nil {
					return err
				}
				updates["phase"] = TaskDispatchReady
				updates["supplier_key"] = key
				updates["next_attempt_at"] = max(dispatch.NextAttemptAt, time.Now().Add(time.Duration(dispatch.BackoffMillis)*time.Millisecond).UnixMilli())
			}
		case "unknown":
			if verified.NewSubmissionSafe || verified.ObservedAt <= 0 ||
				(verified.TaskLookupNotFound && (verified.WatchUpstreamID == "" || verified.FinalCustomerBillingReceiptID == "")) {
				return ErrTaskDispatchConflict
			}
			updates["phase"] = TaskDispatchUnknown
			if dispatch.Phase == TaskDispatchUnknown {
				updates["stage_at"] = dispatch.StageAt
			}
			updates["next_attempt_at"] = max(dispatch.NextAttemptAt, time.Now().Add(dispatch.ReconcileDelay()).UnixMilli())
			if attempt.WatchState == "" {
				attempt.WatchState = "pending"
			}
		default:
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ?", claim.TaskID, claim.Version).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		attemptUpdates := map[string]any{"receipt_kind": verified.Kind, "receipt_id": verified.ReceiptID,
			"evidence_hash": verified.EvidenceHash, "evidence": TaskBlob(evidence),
			"supplier_net_quota":                verified.SupplierNetQuota,
			"supplier_billing_final":            verified.SupplierBillingFinal,
			"new_submission_safe":               verified.NewSubmissionSafe,
			"temporary_technical_cause":         verified.TemporaryTechnicalCause,
			"public_error_code":                 verified.PublicErrorCode,
			"task_lookup_not_found":             verified.TaskLookupNotFound,
			"final_customer_billing_receipt_id": verified.FinalCustomerBillingReceiptID,
			"unknown_observed_at":               verified.ObservedAt}
		if verified.Kind == "not_accepted" && dispatch.Attempt >= dispatch.AttemptBudget && !dispatch.UncertainReplacementUsed {
			code := verified.PublicErrorCode
			if code == "" || len(code) > 64 || strings.Trim(code, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
				code = "provider_error"
			}
			attemptUpdates["public_error_code"] = code
		}
		if verified.Kind == "unknown" {
			attemptUpdates["watch_state"] = attempt.WatchState
			if verified.WatchUpstreamID != "" {
				attemptUpdates["watch_upstream_id"] = verified.WatchUpstreamID
			}
			attemptUpdates["reconcile_next_at"] = gorm.Expr("CASE WHEN reconcile_next_at > ? THEN reconcile_next_at ELSE ? END",
				time.Now().Add(dispatch.ReconcileDelay()).UnixMilli(), time.Now().Add(dispatch.ReconcileDelay()).UnixMilli())
		}
		if verified.Kind == "accepted" && dispatch.UncertainReplacementUsed {
			attemptUpdates["watch_state"] = "pending"
			attemptUpdates["watch_upstream_id"] = verified.UpstreamID
			attemptUpdates["reconcile_next_at"] = time.Now().Add(dispatch.ReconcileDelay()).UnixMilli()
		}
		if verified.Kind == "rejected" && dispatch.UncertainReplacementUsed {
			attemptUpdates["watch_state"] = "observed"
			attemptUpdates["watch_result_status"] = TaskStatusFailure
			attemptUpdates["watch_public_error_code"] = verified.PublicErrorCode
			attemptUpdates["watch_final_billing_receipt_id"] = verified.FinalCustomerBillingReceiptID
			attemptUpdates["watch_observed_at"] = time.Now().Unix()
		}
		if verified.Kind != "unknown" {
			attemptUpdates["resolved_at"] = time.Now().Unix()
		}
		result = tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND receipt_id = ? AND receipt_kind = ?",
			claim.TaskID, claim.Attempt, attempt.ReceiptID, attempt.ReceiptKind).
			Updates(attemptUpdates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}

// MarkTaskSendUnknown records a timeout or lost response without making any
// assertion about supplier acceptance. Later workers query receipts first.
func MarkTaskSendUnknown(ctx context.Context, claim TaskDispatchClaim) error {
	result := DB.WithContext(ctx).Model(&TaskDispatch{}).
		Where("task_id = ? AND version = ? AND owner = ? AND lease_until > ? AND phase = ?", claim.TaskID,
			claim.Version, claim.Owner, time.Now().UnixMilli(), TaskDispatchSendStarted).
		Updates(map[string]any{"phase": TaskDispatchUnknown, "stage_at": time.Now().Unix(), "version": claim.Version + 1, "owner": "", "lease_until": int64(0)})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

// ResumeParkedTaskDispatch requires an explicit host policy decision and a
// durable reason. It only resumes a verified, unpaid predecessor attempt.
func ResumeParkedTaskDispatch(ctx context.Context, taskID, expectedVersion int64, newBudget int, reason string) error {
	if taskID <= 0 || expectedVersion <= 0 || newBudget < 1 || newBudget > MaxTaskAttemptBudget ||
		strings.TrimSpace(reason) == "" || len(reason) > 255 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != expectedVersion || dispatch.Phase != TaskDispatchParked ||
			newBudget <= dispatch.AttemptBudget || newBudget <= dispatch.Attempt {
			return ErrTaskDispatchConflict
		}
		if dispatch.UncertainReplacementUsed {
			return ErrTaskDispatchConflict
		}
		var last TaskDispatchAttempt
		if err := tx.Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).First(&last).Error; err != nil {
			return err
		}
		if last.ReceiptKind != "not_accepted" || !last.SupplierBillingFinal || last.SupplierNetQuota != 0 ||
			!last.NewSubmissionSafe || !last.TemporaryTechnicalCause || last.ReceiptID == "" {
			return ErrTaskDispatchConflict
		}
		key, err := newTaskSupplierKey()
		if err != nil {
			return err
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND phase = ?", taskID, expectedVersion, TaskDispatchParked).
			Updates(map[string]any{"version": expectedVersion + 1, "phase": TaskDispatchReady, "stage_at": time.Now().Unix(),
				"attempt_budget": newBudget, "resume_reason": reason, "supplier_key": key,
				"next_attempt_at": max(dispatch.NextAttemptAt, time.Now().Add(time.Duration(dispatch.BackoffMillis)*time.Millisecond).UnixMilli())})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		if err := tx.Model(&Task{}).Where("id = ?", taskID).Updates(map[string]any{
			"status": TaskStatusSubmitted, "fail_reason": "", "updated_at": time.Now().Unix(),
		}).Error; err != nil {
			return err
		}
		return nil
	})
}

func InitializeTaskDispatch(tx *gorm.DB, taskID int64, policy TaskRetryPolicy) error {
	if taskID <= 0 {
		return fmt.Errorf("invalid task ID")
	}
	policy, err := normalizeTaskRetryPolicy(policy)
	if err != nil {
		return err
	}
	key, err := newTaskSupplierKey()
	if err != nil {
		return err
	}
	return tx.Create(&TaskDispatch{TaskID: taskID, Version: 1, Phase: TaskDispatchReady, StageAt: time.Now().Unix(), SupplierKey: key,
		AttemptBudget: policy.AttemptBudget, BackoffMillis: policy.BackoffMillis, RetryPolicyVersion: 1,
		ReconcileDelaySeconds: policy.ReconcileDelaySeconds, ReplayInitialDelaySeconds: policy.ReplayInitialDelaySeconds,
		ReplayMaxDelaySeconds: policy.ReplayMaxDelaySeconds, ReplayJitterSeconds: policy.ReplayJitterSeconds,
		UncertainRetryEnabled: policy.UncertainRetryEnabled, UncertainRetryDelaySeconds: policy.UncertainRetryDelaySeconds}).Error
}
