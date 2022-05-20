package cosstore

import (
	"context"
	"encoding/binary"
	"github.com/tencentyun/cos-go-sdk-v5"
	"strconv"
	"strings"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/cosstore/connector"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

const (
	// URIScheme TODO
	URIScheme               = "cos"
	httpScheme              = "http"
	httpsScheme             = "https"
	targetHistoryBlobSize   = 2 * 1024 * 1024 // 2MB
	errEncodeHistory        = "failed to encode history batches"
	errWriteKey             = "failed to write history to cos"
	defaultBlobstoreTimeout = 60 * time.Second
)

type (
	historyArchiver struct {
		cosCli    connector.Client
		container *archiver.HistoryBootstrapContainer
		// only set in test code
		historyIterator archiver.HistoryIterator
	}
	uploadProgress struct {
		BatchIdx      int
		IteratorState []byte
		uploadedSize  int64
		historySize   int64
	}
	getHistoryToken struct {
		CloseFailoverVersion int64
		BatchIdx             int
	}
)

// Only validates the scheme and buckets are passed
func softValidateURI(URI archiver.URI) error {
	if URI.Scheme() != httpScheme && URI.Scheme() != httpsScheme {
		return archiver.ErrURISchemeMismatch
	}
	if len(URI.Hostname()) == 0 {
		return archiver.ErrInvalidURI
	}
	return nil
}

func loadHistoryIterator(
	ctx context.Context,
	request *archiver.ArchiveHistoryRequest,
	historyManager persistence.HistoryManager,
	featureCatalog *archiver.ArchiveFeatureCatalog,
	progress *uploadProgress,
) (historyIterator archiver.HistoryIterator) {
	if featureCatalog.ProgressManager != nil {
		if featureCatalog.ProgressManager.HasProgress(ctx) {
			err := featureCatalog.ProgressManager.LoadProgress(ctx, progress)
			if err == nil {
				historyIterator, err := archiver.NewHistoryIteratorFromState(
					ctx, request, historyManager, targetHistoryBlobSize, progress.IteratorState)
				if err == nil {
					return historyIterator
				}
			}
			progress.IteratorState = nil
			progress.BatchIdx = 0
			progress.historySize = 0
			progress.uploadedSize = 0
		}
	}
	return archiver.NewHistoryIterator(ctx, request, historyManager, targetHistoryBlobSize)
}

func getNextHistoryBlob(ctx context.Context, historyIterator archiver.HistoryIterator) (*archiver.HistoryBlob, error) {
	historyBlob, err := historyIterator.Next()
	op := func() error {
		historyBlob, err = historyIterator.Next()
		return err
	}
	throttleRetry := backoff.NewThrottleRetry(
		backoff.WithRetryPolicy(common.CreatePersistenceRetryPolicy()),
		backoff.WithRetryableError(persistence.IsTransientError),
	)
	for err != nil {
		if contextExpired(ctx) {
			return nil, archiver.ErrContextTimeout
		}
		if !persistence.IsTransientError(err) {
			return nil, err
		}
		err = throttleRetry.Do(ctx, op)
	}
	return historyBlob, nil
}

func saveHistoryIteratorState(ctx context.Context, featureCatalog *archiver.ArchiveFeatureCatalog,
	historyIterator archiver.HistoryIterator, progress *uploadProgress) {
	// Saving history state is a best effort operation. Ignore errors and continue
	if featureCatalog.ProgressManager != nil {
		state, err := historyIterator.GetState()
		if err != nil {
			return
		}
		progress.IteratorState = state
		err = featureCatalog.ProgressManager.RecordProgress(ctx, progress)
		if err != nil {
			return
		}
	}
}

// Archive TODO
func (h *historyArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveHistoryRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	scope := h.container.MetricsClient.Scope(metrics.HistoryArchiverScope, metrics.DomainTag(request.DomainName))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer func() {
		sw.Stop()
		if err != nil {
			if persistence.IsTransientError(err) {
				scope.IncCounter(metrics.HistoryArchiverArchiveTransientErrorCount)
			} else {
				scope.IncCounter(metrics.HistoryArchiverArchiveNonRetryableErrorCount)
				if featureCatalog.NonRetriableError != nil {
					err = featureCatalog.NonRetriableError()
				}
			}
		}
	}()

	logger := archiver.TagLoggerWithArchiveHistoryRequestAndURI(h.container.Logger, request, URI.String())

	if err := softValidateURI(URI); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiver.ErrReasonInvalidURI),
			tag.Error(err))
		return err
	}

	if err := archiver.ValidateHistoryArchiveRequest(request); err != nil {
		logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(
			archiver.ErrReasonInvalidArchiveRequest), tag.Error(err))
		return err
	}

	var progress uploadProgress
	historyIterator := h.historyIterator
	if historyIterator == nil { // will only be set by testing code
		historyIterator = loadHistoryIterator(ctx, request, h.container.HistoryV2Manager, featureCatalog, &progress)
	}
	for historyIterator.HasNext() {
		historyBlob, err := getNextHistoryBlob(ctx, historyIterator)
		if err != nil {
			if common.IsEntityNotExistsError(err) {
				// workflow history no longer exists, may due to duplicated archival signal
				// this may happen even in the middle of iterating history as two archival signals
				// can be processed concurrently.
				logger.Info(archiver.ArchiveSkippedInfoMsg)
				scope.IncCounter(metrics.HistoryArchiverDuplicateArchivalsCount)
				return nil
			}

			logger := logger.WithTags(tag.ArchivalArchiveFailReason(archiver.ErrReasonReadHistory), tag.Error(err))
			if persistence.IsTransientError(err) {
				logger.Error(archiver.ArchiveTransientErrorMsg)
			} else {
				logger.Error(archiver.ArchiveNonRetriableErrorMsg)
			}
			return err
		}

		if archiver.IsHistoryMutated(request, historyBlob.Body, *historyBlob.Header.IsLast, logger) {
			if !featureCatalog.ArchiveIncompleteHistory() {
				return archiver.ErrHistoryMutated
			}
		}

		encodedHistoryBlob, err := encode(historyBlob)
		if err != nil {
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errEncodeHistory), tag.Error(err))
			return err
		}

		key := constructHistoryKey(URI.Path(), request.DomainID, request.WorkflowID, request.RunID, request.CloseFailoverVersion,
			progress.BatchIdx)

		exists, err := h.cosCli.Exist(ctx, URI, key)
		if err != nil {
			// client will retry internally
			logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
			return err
		}
		blobSize := int64(binary.Size(encodedHistoryBlob))
		if exists {
			scope.IncCounter(metrics.HistoryArchiverBlobExistsCount)
		} else {
			if err := h.cosCli.Put(ctx, URI, key, encodedHistoryBlob); err != nil {
				logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(errWriteKey), tag.Error(err))
				return err
			}
			progress.uploadedSize += blobSize
			scope.RecordTimer(metrics.HistoryArchiverBlobSize, time.Duration(blobSize))
		}

		progress.historySize += blobSize
		progress.BatchIdx = progress.BatchIdx + 1
		saveHistoryIteratorState(ctx, featureCatalog, historyIterator, &progress)
	}

	scope.RecordTimer(metrics.HistoryArchiverTotalUploadSize, time.Duration(progress.uploadedSize))
	scope.RecordTimer(metrics.HistoryArchiverHistorySize, time.Duration(progress.historySize))
	scope.IncCounter(metrics.HistoryArchiverArchiveSuccessCount)
	return nil
}

// Get TODO
func (h *historyArchiver) Get(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.GetHistoryRequest,
) (*archiver.GetHistoryResponse, error) {
	if err := softValidateURI(URI); err != nil {
		return nil, &types.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateGetRequest(request); err != nil {
		return nil, &types.BadRequestError{Message: archiver.ErrInvalidGetHistoryRequest.Error()}
	}

	var err error
	var token *getHistoryToken
	if request.NextPageToken != nil {
		token, err = deserializeGetHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, &types.BadRequestError{Message: archiver.ErrNextPageTokenCorrupted.Error()}
		}
	} else if request.CloseFailoverVersion != nil {
		token = &getHistoryToken{
			CloseFailoverVersion: *request.CloseFailoverVersion,
		}
	} else {
		highestVersion, err := h.getHighestVersion(ctx, URI, request)
		if err != nil {
			return nil, &types.BadRequestError{Message: err.Error()}
		}
		token = &getHistoryToken{
			CloseFailoverVersion: *highestVersion,
		}
	}

	response := &archiver.GetHistoryResponse{}
	numOfEvents := 0
	isTruncated := false
	for {
		if numOfEvents >= request.PageSize {
			isTruncated = true
			break
		}
		key := constructHistoryKey(URI.Path(), request.DomainID, request.WorkflowID, request.RunID, token.CloseFailoverVersion,
			token.BatchIdx)
		encodedRecord, err := h.cosCli.Get(ctx, URI, key)
		if err != nil {
			return nil, err
		}

		historyBlob, err := decodeHistoryBlob(encodedRecord)
		if err != nil {
			return nil, &types.InternalServiceError{Message: err.Error()}
		}

		for _, batch := range historyBlob.Body {
			response.HistoryBatches = append(response.HistoryBatches, batch)
			numOfEvents += len(batch.Events)
		}

		if *historyBlob.Header.IsLast {
			break
		}
		token.BatchIdx++
	}

	if isTruncated {
		nextToken, err := serializeToken(token)
		if err != nil {
			return nil, &types.InternalServiceError{Message: err.Error()}
		}
		response.NextPageToken = nextToken
	}

	return response, nil
}

// with XDC(global domain) concept, archival may write different history with the same RunID, with different failoverVersion.
// In that case, the history/runID with the highest failoverVersion wins.
// getHighestVersion look up all archived files to find the highest failoverVersion.
func (h *historyArchiver) getHighestVersion(ctx context.Context, URI archiver.URI,
	request *archiver.GetHistoryRequest) (*int64, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	var prefix = constructHistoryKeyPrefix(URI.Path(), request.DomainID, request.WorkflowID, request.RunID) + "/"
	results, err := h.cosCli.ListObjects(ctx, URI, &cos.BucketGetOptions{Prefix: prefix, Delimiter: "/"})
	if err != nil {
		return nil, err
	}
	var highestVersion *int64

	for _, v := range results.CommonPrefixes {
		var version int64
		version, err = strconv.ParseInt(
			strings.Replace(strings.Replace(v, prefix, "", 1), "/", "", 1), 10, 64)
		if err != nil {
			continue
		}
		if highestVersion == nil || version > *highestVersion {
			highestVersion = &version
		}
	}
	if highestVersion == nil {
		return nil, archiver.ErrHistoryNotExist
	}
	return highestVersion, nil
}

// ValidateURI TODO
func (h *historyArchiver) ValidateURI(URI archiver.URI) error {
	err := softValidateURI(URI)
	if err != nil {
		return err
	}
	exist, err := h.cosCli.BucketExist(context.TODO(), URI)
	if exist {
		return nil
	}
	return err
}

// NewHistoryArchiver TODO
func NewHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
	config *config.COSArchiver,
) (archiver.HistoryArchiver, error) {
	return newHistoryArchiver(container, config, nil), nil
}

func newHistoryArchiver(
	container *archiver.HistoryBootstrapContainer,
	config *config.COSArchiver,
	historyIterator archiver.HistoryIterator) *historyArchiver {
	return &historyArchiver{
		cosCli:          connector.NewClient(context.TODO(), config),
		container:       container,
		historyIterator: historyIterator,
	}
}
