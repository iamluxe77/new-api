package model

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"modernc.org/sqlite"
)

const (
	MaxTaskRawInputBytes        = 128 << 20
	MaxTaskNormalizedInputBytes = 1 << 20
	MaxTaskPluginSourceBytes    = 8 << 20
)

var (
	ErrTaskIntakeConflict = errors.New("task idempotency key conflicts with original request")
	ErrTaskIntakeInvalid  = errors.New("invalid durable task intake")
	ErrTaskIntakeTooLarge = errors.New("durable task input exceeds supported body limit")
	ErrTaskIntakeBusy     = errors.New("durable task intake database is temporarily busy")
)

// TaskBlob maps private bounded bytes to a large binary type on each primary
// database. MySQL's plain BLOB would truncate valid multi-megabyte inputs.
type TaskBlob []byte

func (TaskBlob) GormDataType() string { return "bytes" }

func (b TaskBlob) Value() (driver.Value, error) { return []byte(b), nil }

func (b *TaskBlob) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*b = nil
	case []byte:
		*b = append((*b)[:0], v...)
	case string:
		*b = append((*b)[:0], v...)
	default:
		return fmt.Errorf("unsupported task blob value %T", value)
	}
	return nil
}

func (TaskBlob) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	switch db.Dialector.Name() {
	case "mysql":
		return "LONGBLOB"
	case "postgres":
		return "BYTEA"
	default:
		return "BLOB"
	}
}

// TaskIntake is private primary-DB state. No public DTO may embed this type.
type TaskIntake struct {
	TaskID              int64    `json:"-" gorm:"primaryKey;autoIncrement:false"`
	UserID              int      `json:"-" gorm:"uniqueIndex:idx_task_intake_key,priority:1"`
	TokenID             int      `json:"-" gorm:"uniqueIndex:idx_task_intake_key,priority:2"`
	KeyHash             string   `json:"-" gorm:"type:char(64);uniqueIndex:idx_task_intake_key,priority:3;index:idx_task_intake_key_hash"`
	ClientKeyPresent    bool     `json:"-"`
	Fingerprint         string   `json:"-" gorm:"type:char(64);not null"`
	Method              string   `json:"-" gorm:"type:varchar(12);not null"`
	Path                string   `json:"-" gorm:"type:varchar(512);not null"`
	RawQuery            string   `json:"-" gorm:"type:varchar(4096)"`
	ContentType         string   `json:"-" gorm:"type:varchar(255);not null"`
	SemanticHeaders     TaskBlob `json:"-"`
	RawInput            TaskBlob `json:"-"`
	NormalizedInput     TaskBlob `json:"-"`
	ChannelID           int      `json:"-"`
	AccountID           string   `json:"-" gorm:"type:varchar(191)"`
	SupplierBaseURL     string   `json:"-" gorm:"type:varchar(2048)"`
	CredentialSnapshot  TaskBlob `json:"-"`
	RoutingSnapshot     TaskBlob `json:"-"`
	ModelID             string   `json:"-" gorm:"type:varchar(191)"`
	PluginKey           string   `json:"-" gorm:"type:varchar(191)"`
	PluginVersion       string   `json:"-" gorm:"type:varchar(191)"`
	PluginRowID         int64    `json:"-"`
	PluginSourceHash    string   `json:"-" gorm:"type:char(64)"`
	PluginSource        TaskBlob `json:"-"`
	PluginAPIVersion    int      `json:"-"`
	PluginGeneration    uint64   `json:"-"`
	BillingSnapshot     TaskBlob `json:"-"`
	WebhookIntent       TaskBlob `json:"-"`
	RetryPolicySnapshot TaskBlob `json:"-"`
}

type TaskIntakeInput struct {
	ClientKey string
	Method    string
	Path      string
	// RawQuery excludes the leading '?'; byte-identical query is part of replay identity.
	RawQuery        string
	ContentType     string
	RawInput        []byte
	NormalizedInput []byte
	SemanticHeaders map[string]string
	AccountID       string
	// SupplierBaseURL is selected by the host from its frozen channel routing.
	SupplierBaseURL    string
	CredentialSnapshot []byte
	RoutingSnapshot    []byte
	ModelID            string
	PluginKey          string
	PluginVersion      string
	PluginRowID        int64
	PluginSourceHash   string
	PluginGeneration   uint64
	BillingSnapshot    []byte
	WebhookIntent      []byte
	RetryPolicy        TaskRetryPolicy
	QueryRoutes        []TaskIntakeQueryRouteInput
}

// TaskOriginalRequest contains only the client-supplied request identity.
type TaskOriginalRequest struct {
	ClientKey       string
	Method          string
	Path            string
	RawQuery        string
	ContentType     string
	RawInput        []byte
	SemanticHeaders map[string]string
}

type TaskIntakeClaim struct {
	Task     *Task
	Existing bool
}

// GetTaskIntake returns private original request material for the native
// worker. Callers must not marshal this model into a client response or log.
func GetTaskIntake(ctx context.Context, taskID int64) (*TaskIntake, error) {
	var intake TaskIntake
	if err := DB.WithContext(ctx).Where("task_id = ?", taskID).First(&intake).Error; err != nil {
		return nil, err
	}
	return &intake, nil
}

// HasTaskIntakeClientKey only gates replay on an unmatched plugin route. It
// reveals no task data and does not replace authenticated lookup by owner.
func HasTaskIntakeClientKey(ctx context.Context, clientKey, method, path string) (bool, error) {
	if clientKey == "" || len(clientKey) > 255 {
		return false, nil
	}
	digest := sha256.Sum256([]byte(clientKey))
	var count int64
	err := DB.WithContext(ctx).Model(&TaskIntake{}).
		Where("key_hash = ? AND client_key_present = ? AND method = ? AND path = ?", hex.EncodeToString(digest[:]), true, method, path).
		Limit(1).Count(&count).Error
	return count > 0, err
}

// HasOwnedTaskIntakeClientKey checks replay ownership before body capture.
// Method and path are intentionally omitted so a reused key can fail with 409.
func HasOwnedTaskIntakeClientKey(ctx context.Context, userID, tokenID int, clientKey string) (bool, error) {
	if userID <= 0 || tokenID <= 0 || clientKey == "" || len(clientKey) > 255 {
		return false, nil
	}
	digest := sha256.Sum256([]byte(clientKey))
	var count int64
	err := DB.WithContext(ctx).Model(&TaskIntake{}).
		Where("user_id = ? AND token_id = ? AND key_hash = ? AND client_key_present = ?",
			userID, tokenID, hex.EncodeToString(digest[:]), true).Limit(1).Count(&count).Error
	return count > 0, err
}

// ClaimTaskIntake persists the public task, private input, and reservation in
// one primary-DB transaction. A duplicate is read only after rollback, since
// PostgreSQL aborts the transaction on a unique-key violation.
func ClaimTaskIntake(ctx context.Context, task *Task, input TaskIntakeInput, reservation TaskReservation) (*TaskIntakeClaim, error) {
	if task == nil || task.UserId <= 0 || reservation.TokenID <= 0 || common.BatchUpdateEnabled {
		return nil, ErrTaskIntakeInvalid
	}
	if err := validateTaskIntakeQueryRoutes(input.QueryRoutes); err != nil {
		return nil, err
	}
	original := input.originalRequest()
	keyHash, fingerprint, headerJSON, err := prepareTaskOriginalRequest(original)
	if err != nil {
		return nil, err
	}
	if original.ClientKey != "" {
		claim, err := findTaskIntakeWithRetry(ctx, task.UserId, reservation.TokenID, keyHash, fingerprint)
		if claim != nil || err != nil {
			return claim, err
		}
	}
	if task.ID != 0 || task.TaskID == "" || len(task.TaskID) > 191 ||
		string(task.Platform) != input.PluginKey || len(task.Platform) > 30 ||
		len(task.Group) > 50 || len(task.Action) > 40 || len(task.Progress) > 20 ||
		len(task.Properties.UpstreamModelName) > 191 || len(task.Properties.OriginModelName) > 191 ||
		task.Status != TaskStatusNotStart || task.FinishTime != 0 || task.StartTime != 0 ||
		task.Quota != 0 || task.FailReason != "" || len(task.Data) != 0 || task.Properties.Input != "" ||
		(task.Progress != "" && task.Progress != "0%") ||
		!reflect.DeepEqual(task.PrivateData, TaskPrivateData{}) ||
		len(input.NormalizedInput) > MaxTaskNormalizedInputBytes ||
		len(input.CredentialSnapshot) == 0 || len(input.CredentialSnapshot) > 64<<10 ||
		len(input.RoutingSnapshot) > MaxTaskNormalizedInputBytes || len(input.BillingSnapshot) > MaxTaskNormalizedInputBytes ||
		len(input.WebhookIntent) > 8<<10 ||
		input.ModelID == "" || len(input.ModelID) > 191 || input.AccountID == "" || len(input.AccountID) > 191 ||
		input.SupplierBaseURL == "" || len(input.SupplierBaseURL) > 2048 ||
		len(input.RoutingSnapshot) == 0 || input.PluginKey == "" || len(input.PluginKey) > 128 ||
		input.PluginVersion == "" || len(input.PluginVersion) > 64 ||
		input.PluginRowID <= 0 || len(input.PluginSourceHash) != 64 ||
		task.ChannelId <= 0 || reservation.Quota < 0 || reservation.Quota > common.MaxQuota ||
		strings.ContainsFunc(input.AccountID+input.ModelID+input.PluginKey+input.PluginVersion+input.SupplierBaseURL+task.TaskID+
			string(task.Platform)+task.Group+task.Action+task.Progress+
			task.Properties.UpstreamModelName+task.Properties.OriginModelName, unicode.IsControl) {
		return nil, ErrTaskIntakeInvalid
	}
	baseURL, err := url.Parse(input.SupplierBaseURL)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Hostname() == "" ||
		baseURL.User != nil || baseURL.RawQuery != "" || baseURL.ForceQuery ||
		strings.Contains(input.SupplierBaseURL, "#") || baseURL.Opaque != "" {
		return nil, ErrTaskIntakeInvalid
	}
	var billingSnapshot, routingSnapshot map[string]any
	if len(input.BillingSnapshot) == 0 || common.Unmarshal(input.BillingSnapshot, &billingSnapshot) != nil || billingSnapshot == nil ||
		common.Unmarshal(input.RoutingSnapshot, &routingSnapshot) != nil || routingSnapshot == nil {
		return nil, ErrTaskIntakeInvalid
	}
	key := original.ClientKey
	if key == "" {
		var randomKey [32]byte
		if _, err := rand.Read(randomKey[:]); err != nil {
			return nil, err
		}
		keyHash = hex.EncodeToString(randomKey[:])
	}
	policy, err := normalizeTaskRetryPolicy(input.RetryPolicy)
	if err != nil {
		return nil, ErrTaskIntakeInvalid
	}
	policyJSON, err := common.Marshal(policy)
	if err != nil {
		return nil, ErrTaskIntakeInvalid
	}
	intake := &TaskIntake{
		UserID: task.UserId, TokenID: reservation.TokenID, KeyHash: keyHash,
		ClientKeyPresent: key != "", Fingerprint: fingerprint,
		Method: input.Method, Path: input.Path, RawQuery: input.RawQuery, ContentType: input.ContentType,
		SemanticHeaders: TaskBlob(headerJSON),
		RawInput:        TaskBlob(input.RawInput), NormalizedInput: TaskBlob(input.NormalizedInput),
		ChannelID: task.ChannelId, AccountID: input.AccountID, SupplierBaseURL: input.SupplierBaseURL,
		CredentialSnapshot: TaskBlob(input.CredentialSnapshot),
		RoutingSnapshot:    TaskBlob(input.RoutingSnapshot), ModelID: input.ModelID,
		PluginKey: input.PluginKey, PluginVersion: input.PluginVersion,
		PluginRowID: input.PluginRowID, PluginSourceHash: input.PluginSourceHash,
		PluginGeneration: input.PluginGeneration, BillingSnapshot: TaskBlob(input.BillingSnapshot),
		WebhookIntent: TaskBlob(input.WebhookIntent), RetryPolicySnapshot: TaskBlob(policyJSON),
	}
	const maxSQLiteAttempts = 10
	for attempt := range maxSQLiteAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if key != "" {
			claim, lookupErr := findExistingTaskIntake(ctx, task.UserId, reservation.TokenID, keyHash, fingerprint)
			if claim != nil || (lookupErr != nil && !isTaskIntakeSQLiteBusy(lookupErr)) {
				return claim, lookupErr
			}
			if lookupErr != nil {
				err = lookupErr
				if err := waitTaskIntakeSQLiteRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
		}
		candidate := *task
		row := *intake
		err = DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var plugin TaskPlugin
			if err := tx.Select("id", "key", "version", "source_hash", "source", "api_version").
				Where("id = ? AND key = ? AND version = ? AND source_hash = ?",
					input.PluginRowID, input.PluginKey, input.PluginVersion, input.PluginSourceHash).First(&plugin).Error; err != nil {
				return err
			}
			source := []byte(plugin.Source)
			sourceDigest := sha256.Sum256(source)
			if len(source) == 0 || len(source) > MaxTaskPluginSourceBytes || plugin.APIVersion <= 0 ||
				hex.EncodeToString(sourceDigest[:]) != input.PluginSourceHash {
				return ErrTaskIntakeInvalid
			}
			row.PluginSource = TaskBlob(source)
			row.PluginAPIVersion = plugin.APIVersion
			if err := tx.Create(&candidate).Error; err != nil {
				return err
			}
			row.TaskID = candidate.ID
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
			for _, binding := range input.QueryRoutes {
				if err := tx.Create(&TaskIntakeQueryRoute{
					TaskID: candidate.ID, PathHash: taskIntakeQueryHash(binding.Path),
					Path: binding.Path, Render: binding.Render, TaskIDParam: binding.TaskIDParam,
				}).Error; err != nil {
					return err
				}
			}
			if err := reserveTaskFinance(tx, &candidate, reservation, input.BillingSnapshot); err != nil {
				return err
			}
			return InitializeTaskDispatch(tx, candidate.ID, policy)
		})
		if err == nil {
			*task = candidate
			invalidateTaskFinanceCaches(task.UserId, reservation.TokenID)
			return &TaskIntakeClaim{Task: task}, nil
		}
		// Always leave the failed transaction before reading a competing winner.
		if key != "" {
			claim, lookupErr := findExistingTaskIntake(ctx, task.UserId, reservation.TokenID, keyHash, fingerprint)
			if claim != nil || (lookupErr != nil && !isTaskIntakeSQLiteBusy(lookupErr)) {
				return claim, lookupErr
			}
			if isTaskIntakeSQLiteBusy(lookupErr) {
				err = lookupErr
			}
		}
		if !isTaskIntakeSQLiteBusy(err) {
			return nil, err
		}
		if err := waitTaskIntakeSQLiteRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key != "" {
		claim, lookupErr := findExistingTaskIntake(ctx, task.UserId, reservation.TokenID, keyHash, fingerprint)
		if claim != nil || (lookupErr != nil && !isTaskIntakeSQLiteBusy(lookupErr)) {
			return claim, lookupErr
		}
		if lookupErr != nil {
			err = lookupErr
		}
	}
	if isTaskIntakeSQLiteBusy(err) {
		return nil, fmt.Errorf("%w: %v", ErrTaskIntakeBusy, err)
	}
	return nil, err
}

func (input TaskIntakeInput) originalRequest() TaskOriginalRequest {
	return TaskOriginalRequest{
		ClientKey: input.ClientKey, Method: input.Method, Path: input.Path,
		RawQuery: input.RawQuery, ContentType: input.ContentType,
		RawInput: input.RawInput, SemanticHeaders: input.SemanticHeaders,
	}
}

// FindTaskIntakeByClientKey returns the original task without consulting current
// channel, plugin, credential, or pricing state. A keyless request has no replay.
func FindTaskIntakeByClientKey(ctx context.Context, userID, tokenID int, original TaskOriginalRequest) (*TaskIntakeClaim, error) {
	if original.ClientKey == "" {
		return nil, nil
	}
	if userID <= 0 || tokenID <= 0 {
		return nil, ErrTaskIntakeInvalid
	}
	keyHash, fingerprint, _, err := prepareTaskOriginalRequest(original)
	if err != nil {
		return nil, err
	}
	return findTaskIntakeWithRetry(ctx, userID, tokenID, keyHash, fingerprint)
}

func prepareTaskOriginalRequest(original TaskOriginalRequest) (keyHash, fingerprint string, headerJSON []byte, err error) {
	if len(original.RawInput) > MaxTaskRawInputBytes {
		return "", "", nil, ErrTaskIntakeTooLarge
	}
	if strings.TrimSpace(original.Method) == "" || strings.TrimSpace(original.Path) == "" ||
		len(original.Method) > 12 || len(original.Path) > 512 || strings.Contains(original.Path, "?") ||
		len(original.RawQuery) > 4096 || strings.HasPrefix(original.RawQuery, "?") ||
		len(original.ContentType) > 255 || len(original.ClientKey) > 255 ||
		original.ClientKey != strings.TrimSpace(original.ClientKey) ||
		strings.ContainsFunc(original.Method+original.Path+original.RawQuery+original.ContentType+original.ClientKey, unicode.IsControl) ||
		len(original.SemanticHeaders) > 64 {
		return "", "", nil, ErrTaskIntakeInvalid
	}
	canonicalHeaders := make(map[string]string, len(original.SemanticHeaders))
	for header, value := range original.SemanticHeaders {
		canonical := strings.ToLower(header)
		if canonical == "" || len(canonical) > 128 || len(value) > 4096 ||
			strings.ContainsFunc(header+value, unicode.IsControl) {
			return "", "", nil, ErrTaskIntakeInvalid
		}
		if _, exists := canonicalHeaders[canonical]; exists {
			return "", "", nil, ErrTaskIntakeInvalid
		}
		canonicalHeaders[canonical] = value
	}
	headerJSON, err = common.Marshal(canonicalHeaders)
	if err != nil {
		return "", "", nil, err
	}
	fingerprint = taskInputFingerprint(original)
	if original.ClientKey != "" {
		hash := sha256.Sum256([]byte(original.ClientKey))
		keyHash = hex.EncodeToString(hash[:])
	}
	return keyHash, fingerprint, headerJSON, nil
}

func findTaskIntakeWithRetry(ctx context.Context, userID, tokenID int, keyHash, fingerprint string) (*TaskIntakeClaim, error) {
	const maxSQLiteAttempts = 10
	for attempt := range maxSQLiteAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		claim, err := findExistingTaskIntake(ctx, userID, tokenID, keyHash, fingerprint)
		if !isTaskIntakeSQLiteBusy(err) {
			return claim, err
		}
		if err := waitTaskIntakeSQLiteRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	claim, err := findExistingTaskIntake(ctx, userID, tokenID, keyHash, fingerprint)
	if isTaskIntakeSQLiteBusy(err) {
		return nil, fmt.Errorf("%w: %v", ErrTaskIntakeBusy, err)
	}
	return claim, err
}

func isTaskIntakeSQLiteBusy(err error) bool {
	if !common.UsingMainDatabase(common.DatabaseTypeSQLite) {
		return false
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff
	return code == 5 || code == 6 // SQLITE_BUSY or SQLITE_LOCKED, including extended codes.
}

func waitTaskIntakeSQLiteRetry(ctx context.Context, attempt int) error {
	delay := min(time.Duration(20*(1<<min(attempt, 4)))*time.Millisecond, 250*time.Millisecond)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func findExistingTaskIntake(ctx context.Context, userID, tokenID int, keyHash, fingerprint string) (*TaskIntakeClaim, error) {
	var intake TaskIntake
	err := DB.WithContext(ctx).Where("user_id = ? AND token_id = ? AND key_hash = ?", userID, tokenID, keyHash).First(&intake).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if intake.Fingerprint != fingerprint {
		return nil, ErrTaskIntakeConflict
	}
	var task Task
	if err := DB.WithContext(ctx).First(&task, intake.TaskID).Error; err != nil {
		return nil, err
	}
	return &TaskIntakeClaim{Task: &task, Existing: true}, nil
}

func taskInputFingerprint(input TaskOriginalRequest) string {
	h := sha256.New()
	write := func(value []byte) {
		_, _ = fmt.Fprintf(h, "%d:", len(value))
		_, _ = h.Write(value)
	}
	write([]byte(input.Method))
	write([]byte(input.Path))
	write([]byte(input.RawQuery))
	write([]byte(input.ContentType))
	write(input.RawInput)
	keys := make([]string, 0, len(input.SemanticHeaders))
	for key := range input.SemanticHeaders {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return strings.ToLower(keys[i]) < strings.ToLower(keys[j]) })
	for _, key := range keys {
		write([]byte(strings.ToLower(key)))
		write([]byte(input.SemanticHeaders[key]))
	}
	return hex.EncodeToString(h.Sum(nil))
}
