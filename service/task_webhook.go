package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
)

var webhookCursor atomic.Value

func RunTaskWebhookPass(ctx context.Context, seen map[int64]bool) bool {
	if ctx.Err() != nil {
		return false
	}
	cursor, _ := webhookCursor.Load().(string)
	rows, err := model.ListDueTaskWebhooks(ctx, cursor, 4)
	if err != nil {
		logger.LogError(ctx, "task webhook page failed: %v", err)
		return false
	}
	if len(rows) == 0 {
		webhookCursor.Store("")
		return true
	}
	for _, row := range rows {
		webhookCursor.Store(row.EventID)
		if seen[row.TaskID] {
			continue
		}
		seen[row.TaskID] = true
		var owner [16]byte
		if _, err := rand.Read(owner[:]); err != nil {
			return false
		}
		claim, err := model.ClaimTaskWebhook(ctx, row.EventID, hex.EncodeToString(owner[:]))
		if errors.Is(err, model.ErrTaskWebhookConflict) {
			continue
		}
		if err != nil {
			logger.LogError(ctx, "task webhook claim failed: %v", err)
			return false
		}
		operationCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = deliverTaskWebhook(operationCtx, claim)
		cancel()
		if err != nil {
			logger.LogError(ctx, "task webhook delivery failed: %v", err)
			return false
		}
		break // One HTTP command under this native scheduler pass.
	}
	return true
}

func deliverTaskWebhook(ctx context.Context, delivery *model.TaskWebhookDelivery) error {
	if delivery.BodyHash == "" {
		body, publicBody, err := RenderDurableTaskWebhook(ctx, delivery.TaskID, delivery.EventID)
		if err != nil {
			_ = model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "pending", 0, time.Now().Add(time.Minute).Unix())
			return err
		}
		if err := model.FreezeTaskWebhookBody(ctx, delivery, body, publicBody); err != nil {
			return err
		}
	}
	bodyHash := sha256.Sum256(delivery.Body)
	if len(delivery.Body) == 0 || len(delivery.Body) > 1<<20 ||
		hex.EncodeToString(bodyHash[:]) != delivery.BodyHash {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
	}
	strict := &common.SSRFProtection{ApplyIPFilterForDomain: true}
	target, err := url.Parse(delivery.URL)
	if err != nil || target.Scheme != "https" || target.Hostname() == "" {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
	}
	port := 443
	if target.Port() != "" {
		port, err = strconv.Atoi(target.Port())
		if err != nil {
			return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
		}
	}
	if err := strict.ValidateNetworkTarget(target.Hostname(), port); err != nil {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, target.Hostname())
	if err != nil || len(addresses) == 0 {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "pending", 0,
			time.Now().Add(taskWebhookRetryDelay(delivery.AttemptCount, "")).Unix())
	}
	for _, address := range addresses {
		if err := strict.ValidateResolvedIP(target.Hostname(), address.IP); err != nil {
			return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
		}
	}
	var headers map[string]string
	if err := common.Unmarshal(delivery.Headers, &headers); err != nil {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.URL, bytes.NewReader(delivery.Body))
	if err != nil {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", 0, 0)
	}
	req.GetBody = nil
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-New-Api-Callback-Event-Id", delivery.EventID)
	req.Header.Set("X-New-Api-Callback-Attempt", strconv.Itoa(delivery.AttemptCount))
	client := newProtectedFetchHTTPClientWithProxy(nil, nil,
		func() (*common.SSRFProtection, bool, error) { return strict, true, nil },
		func(*http.Request) (*url.URL, error) { return nil, nil })
	client.Timeout = 15 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "pending", 0,
			time.Now().Add(taskWebhookRetryDelay(delivery.AttemptCount, "")).Unix())
	}
	defer resp.Body.Close()
	readBytes, readErr := io.CopyN(io.Discard, resp.Body, 4097)
	if readBytes > 4096 || readErr != nil && !errors.Is(readErr, io.EOF) {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "pending", resp.StatusCode,
			time.Now().Add(taskWebhookRetryDelay(delivery.AttemptCount, resp.Header.Get("Retry-After"))).Unix())
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "delivered", resp.StatusCode, 0)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "pending", resp.StatusCode,
			time.Now().Add(taskWebhookRetryDelay(delivery.AttemptCount, resp.Header.Get("Retry-After"))).Unix())
	}
	return model.FinishTaskWebhook(context.WithoutCancel(ctx), delivery, "blocked", resp.StatusCode, 0)
}

func taskWebhookRetryDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && seconds > 0 {
		return time.Duration(min(seconds, 3600)) * time.Second
	}
	if date, err := http.ParseTime(retryAfter); err == nil && date.After(time.Now()) {
		return min(time.Until(date), time.Hour)
	}
	return time.Duration(min(int64(15)<<min(attempt, 8), int64(3600))) * time.Second
}
