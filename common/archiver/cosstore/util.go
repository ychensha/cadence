package cosstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/types"
)

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeHistoryBlob(data []byte) (*archiver.HistoryBlob, error) {
	historyBlob := &archiver.HistoryBlob{}
	err := json.Unmarshal(data, historyBlob)
	if err != nil {
		return nil, err
	}
	return historyBlob, nil
}

func decodeVisibilityRecord(data []byte) (*visibilityRecord, error) {
	record := &visibilityRecord{}
	err := json.Unmarshal(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func deserializeQueryVisibilityToken(bytes []byte) *string {
	var ret = string(bytes)
	return &ret
}
func serializeQueryVisibilityToken(token string) []byte {
	return []byte(token)
}

func ensureContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultBlobstoreTimeout)
}

// Key construction
func constructHistoryKey(path, domainID, workflowID, runID string, version int64, batchIdx int) string {
	prefix := constructHistoryKeyPrefixWithVersion(path, domainID, workflowID, runID, version)
	return fmt.Sprintf("%s%d", prefix, batchIdx)
}

func constructHistoryKeyPrefixWithVersion(path, domainID, workflowID, runID string, version int64) string {
	prefix := constructHistoryKeyPrefix(path, domainID, workflowID, runID)
	return fmt.Sprintf("%s/%v/", prefix, version)
}

func constructHistoryKeyPrefix(path, domainID, workflowID, runID string) string {
	return strings.TrimLeft(strings.Join([]string{path, domainID, "history", workflowID, runID}, "/"), "/")
}

func constructTimeBasedSearchKey(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, timestamp int64, precision string) string {
	t := time.Unix(0, timestamp).In(time.UTC)
	var timeFormat = ""
	switch precision {
	case PrecisionSecond:
		timeFormat = ":05"
		fallthrough
	case PrecisionMinute:
		timeFormat = ":04" + timeFormat
		fallthrough
	case PrecisionHour:
		timeFormat = "15" + timeFormat
		fallthrough
	case PrecisionDay:
		timeFormat = "2006-01-02T" + timeFormat
	}

	return fmt.Sprintf("%s/%s", constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), t.Format(timeFormat))
}

func constructTimestampIndex(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, timestamp int64, runID string) string {
	t := time.Unix(0, timestamp).In(time.UTC)
	return fmt.Sprintf("%s/%s/%s", constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexKey), t.Format(time.RFC3339), runID)
}

func constructVisibilitySearchPrefix(path, domainID, primaryIndexKey, primaryIndexValue, secondaryIndexType string) string {
	return strings.TrimLeft(strings.Join([]string{path, domainID, "visibility", primaryIndexKey, primaryIndexValue, secondaryIndexType}, "/"), "/")
}

func convertToExecutionInfo(record *visibilityRecord) *types.WorkflowExecutionInfo {
	return &types.WorkflowExecutionInfo{
		Execution: &types.WorkflowExecution{
			WorkflowID: record.WorkflowID,
			RunID:      record.RunID,
		},
		Type: &types.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:     common.Int64Ptr(record.StartTimestamp),
		ExecutionTime: common.Int64Ptr(record.ExecutionTimestamp),
		CloseTime:     common.Int64Ptr(record.CloseTimestamp),
		CloseStatus:   record.CloseStatus.Ptr(),
		HistoryLength: record.HistoryLength,
		Memo:          record.Memo,
		SearchAttributes: &types.SearchAttributes{
			IndexedFields: archiver.ConvertSearchAttrToBytes(record.SearchAttributes),
		},
	}
}
