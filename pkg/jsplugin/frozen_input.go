package jsplugin

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
)

const ContextKeyFrozenTaskFiles = "task_plugin_frozen_files"

const (
	maxFrozenParts      = 256
	maxFrozenFiles      = 32
	maxFrozenFieldBytes = 1 << 20
)

type FrozenTaskFile struct {
	Filename string
	MIMEType string
	Bytes    []byte
}

// ParseFrozenTaskBody keeps durable multipart bytes request-local. Unlike
// multipart.ReadForm, it never spills customer input to a temporary file.
func ParseFrozenTaskBody(raw []byte, contentType string) (map[string]any, []map[string]any, map[string][]FrozenTaskFile, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, nil, nil, err
	}
	switch {
	case mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"):
		if !utf8.Valid(raw) {
			return nil, nil, nil, errors.New("JSON body must be valid UTF-8")
		}
		var value any
		if err := common.Unmarshal(raw, &value); err != nil {
			return nil, nil, nil, err
		}
		return map[string]any{"kind": string(BodyJSON), "value": value}, nil, nil, nil
	case mediaType == "application/x-www-form-urlencoded":
		if !utf8.Valid(raw) {
			return nil, nil, nil, errors.New("form body must be valid UTF-8")
		}
		fields, err := url.ParseQuery(string(raw))
		if err != nil || len(fields) > maxFrozenParts {
			return nil, nil, nil, errors.New("invalid frozen form fields")
		}
		if err := validateFrozenFields(fields); err != nil {
			return nil, nil, nil, err
		}
		return map[string]any{"kind": string(BodyForm), "fields": fields}, nil, nil, nil
	case mediaType != "multipart/form-data":
		return nil, nil, nil, fmt.Errorf("unsupported frozen media type %q", mediaType)
	}
	if params["boundary"] == "" {
		return nil, nil, nil, errors.New("multipart boundary is required")
	}
	reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	fields := make(url.Values)
	files := make(map[string][]FrozenTaskFile)
	parts, fileCount := 0, 0
	fileLimitMB := constant.MaxFileDownloadMB
	if fileLimitMB <= 0 || fileLimitMB > 64 {
		fileLimitMB = 64
	}
	fileLimit := int64(fileLimitMB) << 20
	var fileTotal int64
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, nil, err
		}
		parts++
		name, filename := part.FormName(), part.FileName()
		if parts > maxFrozenParts || !utf8.ValidString(name) || len(name) == 0 || len(name) > 256 || strings.Contains(name, "#") ||
			!utf8.ValidString(filename) || len(filename) > 255 {
			part.Close()
			return nil, nil, nil, errors.New("invalid frozen multipart part")
		}
		partType := part.Header.Get("Content-Type")
		if partType != "" {
			parsed, _, parseErr := mime.ParseMediaType(partType)
			if parseErr != nil || strings.HasPrefix(strings.ToLower(parsed), "multipart/") {
				part.Close()
				return nil, nil, nil, errors.New("invalid frozen multipart Content-Type")
			}
		}
		limit := int64(maxFrozenFieldBytes)
		if filename != "" {
			fileCount++
			limit = fileLimit
		}
		if fileCount > maxFrozenFiles {
			part.Close()
			return nil, nil, nil, errors.New("too many frozen multipart files")
		}
		data, readErr := io.ReadAll(io.LimitReader(part, limit+1))
		part.Close()
		if readErr != nil || int64(len(data)) > limit {
			return nil, nil, nil, errors.New("frozen multipart part exceeds limit")
		}
		if filename == "" {
			if !utf8.Valid(data) {
				return nil, nil, nil, errors.New("multipart field must be valid UTF-8")
			}
			fields[name] = append(fields[name], string(data))
		} else {
			fileTotal += int64(len(data))
			if fileTotal > fileLimit {
				return nil, nil, nil, errors.New("frozen multipart files exceed limit")
			}
			files[name] = append(files[name], FrozenTaskFile{Filename: filename, MIMEType: partType, Bytes: data})
		}
	}
	if err := validateFrozenFields(fields); err != nil {
		return nil, nil, nil, err
	}
	keys := make([]string, 0, len(files))
	for name := range files {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	descriptors := make([]map[string]any, 0, fileCount)
	for _, name := range keys {
		for index, file := range files[name] {
			descriptors = append(descriptors, map[string]any{"ref": FileReference(name, index), "field": name,
				"filename": file.Filename, "mimeType": file.MIMEType, "size": len(file.Bytes)})
		}
	}
	return map[string]any{"kind": string(BodyMultipart), "fields": fields, "files": descriptors}, descriptors, files, nil
}

func validateFrozenFields(fields url.Values) error {
	count := 0
	for name, values := range fields {
		if !utf8.ValidString(name) || len(name) == 0 || len(name) > 256 {
			return errors.New("invalid frozen field name")
		}
		for _, value := range values {
			count++
			if count > maxFrozenParts || !utf8.ValidString(value) || len(value) > maxFrozenFieldBytes {
				return errors.New("invalid frozen field value")
			}
		}
	}
	return nil
}
