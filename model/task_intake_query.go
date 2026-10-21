package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"gorm.io/gorm"
)

// TaskIntakeQueryRoute binds one accepted task to an exact frozen native GET
// path. PathHash keeps lookup indexed without a database-specific long index.
type TaskIntakeQueryRoute struct {
	ID          int64  `gorm:"primaryKey"`
	TaskID      int64  `gorm:"index:idx_task_intake_query_task;not null"`
	PathHash    string `gorm:"type:char(64);uniqueIndex:idx_task_intake_query_path;not null"`
	Path        string `gorm:"type:varchar(512);not null"`
	Render      string `gorm:"type:varchar(128);not null"`
	TaskIDParam string `gorm:"type:varchar(128);not null"`
}

type TaskIntakeQueryRouteInput struct {
	Path        string
	Render      string
	TaskIDParam string
}

func taskIntakeQueryHash(path string) string {
	digest := sha256.Sum256([]byte(http.MethodGet + "\x00" + path))
	return hex.EncodeToString(digest[:])
}

func validateTaskIntakeQueryRoutes(routes []TaskIntakeQueryRouteInput) error {
	if len(routes) > 8 {
		return ErrTaskIntakeInvalid
	}
	seen := make(map[string]bool, len(routes))
	for _, route := range routes {
		if !strings.HasPrefix(route.Path, "/") || len(route.Path) > 512 || route.Render == "" ||
			len(route.Render) > 128 || route.TaskIDParam == "" || len(route.TaskIDParam) > 128 || seen[route.Path] {
			return ErrTaskIntakeInvalid
		}
		seen[route.Path] = true
	}
	return nil
}

// FindTaskIntakeQueryRoute is only a route match. The caller must authenticate
// and check task ownership before rendering or exposing task state.
func FindTaskIntakeQueryRoute(ctx context.Context, method, path string) (*TaskIntakeQueryRoute, error) {
	if method != http.MethodGet || path == "" || len(path) > 512 {
		return nil, gorm.ErrRecordNotFound
	}
	var route TaskIntakeQueryRoute
	err := DB.WithContext(ctx).Where("path_hash = ? AND path = ?", taskIntakeQueryHash(path), path).First(&route).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, gorm.ErrRecordNotFound
	}
	return &route, err
}
