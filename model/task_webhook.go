package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

var ErrTaskWebhookConflict = errors.New("task webhook delivery state changed")

// TaskWebhookIntent is a credentialless decoder result. Its headers are client
// callback headers, never copied from a channel or supplier request.
type TaskWebhookIntent struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

func ValidateTaskWebhookIntent(encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > 8<<10 {
		return ErrTaskIntakeInvalid
	}
	var intent TaskWebhookIntent
	if err := common.Unmarshal(encoded, &intent); err != nil {
		return ErrTaskIntakeInvalid
	}
	u, err := url.Parse(intent.URL)
	if err != nil || len(intent.URL) > 2048 || u.Scheme != "https" || u.Hostname() == "" ||
		u.User != nil || u.Opaque != "" || u.Fragment != "" || strings.ContainsFunc(intent.URL, unicode.IsControl) {
		return ErrTaskIntakeInvalid
	}
	if len(intent.Headers) > 8 {
		return ErrTaskIntakeInvalid
	}
	for name, value := range intent.Headers {
		lower := strings.ToLower(name)
		if lower != "authorization" && !strings.HasPrefix(lower, "x-") ||
			lower == "x-new-api-callback-event-id" || lower == "x-new-api-callback-attempt" ||
			len(name) > 80 || len(value) > 4096 ||
			strings.ContainsFunc(name+value, unicode.IsControl) || strings.ContainsAny(name, " :/\\") {
			return ErrTaskIntakeInvalid
		}
	}
	return nil
}

// TaskWebhookDelivery is a private Task companion. EventID is public and
// deterministic; URL, headers and body are never included in task DTOs.
type TaskWebhookDelivery struct {
	EventID       string   `gorm:"primaryKey;type:varchar(100)"`
	TaskID        int64    `gorm:"uniqueIndex"`
	URL           string   `json:"-" gorm:"type:text;not null"`
	Headers       TaskBlob `json:"-"`
	Body          TaskBlob `json:"-"`
	PublicBody    TaskBlob `json:"-"`
	BodyHash      string   `json:"-" gorm:"type:char(64)"`
	State         string   `gorm:"type:varchar(20);index"`
	CreatedAt     int64
	DeadlineAt    int64
	NextAttemptAt int64 `gorm:"index"`
	AttemptCount  int
	Version       int64
	LeaseOwner    string `json:"-" gorm:"type:varchar(64)"`
	LeaseUntil    int64
	AcceptedAt    int64
	LastHTTPCode  int
}

func createTaskWebhookDeliveryTx(tx *gorm.DB, taskID int64, eventID string, now int64) error {
	var intake TaskIntake
	if err := tx.Select("task_id", "webhook_intent").Where("task_id = ?", taskID).Take(&intake).Error; err != nil {
		return err
	}
	if len(intake.WebhookIntent) == 0 {
		return nil
	}
	if err := ValidateTaskWebhookIntent(intake.WebhookIntent); err != nil {
		return err
	}
	var intent TaskWebhookIntent
	if err := common.Unmarshal(intake.WebhookIntent, &intent); err != nil {
		return err
	}
	headers, err := common.Marshal(intent.Headers)
	if err != nil {
		return err
	}
	return tx.Create(&TaskWebhookDelivery{EventID: eventID, TaskID: taskID,
		URL: intent.URL, Headers: TaskBlob(headers), State: "pending", CreatedAt: now,
		DeadlineAt: now + 24*3600, NextAttemptAt: now, Version: 1}).Error
}

func HasTaskWebhookBacklog(ctx context.Context) (bool, error) {
	var count int64
	err := DB.WithContext(ctx).Model(&TaskWebhookDelivery{}).
		Where("state = ?", "pending").Limit(1).Count(&count).Error
	return count > 0, err
}

func ListDueTaskWebhooks(ctx context.Context, afterEventID string, limit int) ([]TaskWebhookDelivery, error) {
	if limit < 1 || limit > 100 {
		limit = 4
	}
	var rows []TaskWebhookDelivery
	err := DB.WithContext(ctx).Where("event_id > ? AND state = ? AND next_attempt_at <= ? AND lease_until <= ?",
		afterEventID, "pending", time.Now().Unix(), time.Now().Unix()).Order("event_id asc").Limit(limit).Find(&rows).Error
	return rows, err
}

func ClaimTaskWebhook(ctx context.Context, eventID, owner string) (*TaskWebhookDelivery, error) {
	if eventID == "" || len(owner) < 8 || len(owner) > 64 {
		return nil, ErrTaskWebhookConflict
	}
	var claimed TaskWebhookDelivery
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockForUpdate(tx).Where("event_id = ?", eventID).Take(&claimed).Error; err != nil {
			return err
		}
		now := time.Now().Unix()
		if claimed.State != "pending" || claimed.NextAttemptAt > now || claimed.LeaseUntil > now {
			return ErrTaskWebhookConflict
		}
		if now >= claimed.DeadlineAt {
			if err := tx.Model(&claimed).Updates(map[string]any{"state": "expired", "version": claimed.Version + 1}).Error; err != nil {
				return err
			}
			claimed.State = "expired"
			return nil
		}
		result := tx.Model(&TaskWebhookDelivery{}).Where("event_id = ? AND version = ?", eventID, claimed.Version).
			Updates(map[string]any{"lease_owner": owner, "lease_until": now + 45, "version": claimed.Version + 1,
				"attempt_count": claimed.AttemptCount + 1})
		if result.Error != nil || result.RowsAffected != 1 {
			return ErrTaskWebhookConflict
		}
		claimed.LeaseOwner, claimed.LeaseUntil, claimed.Version, claimed.AttemptCount = owner, now+45, claimed.Version+1, claimed.AttemptCount+1
		return nil
	})
	if err != nil {
		return nil, err
	}
	if claimed.State == "expired" {
		return nil, ErrTaskWebhookConflict
	}
	return &claimed, nil
}

func FreezeTaskWebhookBody(ctx context.Context, delivery *TaskWebhookDelivery, body, publicBody []byte) error {
	if delivery == nil || len(body) == 0 || len(body) > 1<<20 || len(publicBody) == 0 || len(publicBody) > 1<<20 {
		return ErrTaskWebhookConflict
	}
	hash := sha256.Sum256(body)
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current TaskWebhookDelivery
		if err := lockForUpdate(tx).Where("event_id = ?", delivery.EventID).Take(&current).Error; err != nil {
			return err
		}
		if current.State != "pending" || current.LeaseOwner != delivery.LeaseOwner || current.Version != delivery.Version {
			return ErrTaskWebhookConflict
		}
		if current.BodyHash != "" {
			if current.BodyHash != hex.EncodeToString(hash[:]) {
				return ErrTaskWebhookConflict
			}
			return nil
		}
		if err := tx.Model(&current).Updates(map[string]any{"body": TaskBlob(body), "public_body": TaskBlob(publicBody), "body_hash": hex.EncodeToString(hash[:]),
			"version": current.Version + 1}).Error; err != nil {
			return err
		}
		delivery.Body, delivery.PublicBody, delivery.BodyHash, delivery.Version = TaskBlob(body), TaskBlob(publicBody), hex.EncodeToString(hash[:]), current.Version+1
		return nil
	})
}

func FinishTaskWebhook(ctx context.Context, delivery *TaskWebhookDelivery, state string, statusCode int, nextAttemptAt int64) error {
	if delivery == nil || (state != "pending" && state != "delivered" && state != "blocked" && state != "expired") {
		return ErrTaskWebhookConflict
	}
	now := time.Now().Unix()
	updates := map[string]any{"state": state, "last_http_code": statusCode, "lease_owner": "", "lease_until": 0,
		"version": delivery.Version + 1}
	if state == "delivered" {
		updates["accepted_at"] = now
	} else if state == "pending" {
		updates["next_attempt_at"] = nextAttemptAt
	}
	result := DB.WithContext(ctx).Model(&TaskWebhookDelivery{}).Where("event_id = ? AND state = ? AND version = ? AND lease_owner = ?",
		delivery.EventID, "pending", delivery.Version, delivery.LeaseOwner).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskWebhookConflict
	}
	return nil
}
