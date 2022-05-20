package cosstore

import (
	"context"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/cosstore/connector"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

type (
	visibilityArchiver struct {
		container   *archiver.VisibilityBootstrapContainer
		cosCli      connector.Client
		queryParser QueryParser
	}

	visibilityRecord archiver.ArchiveVisibilityRequest

	queryVisibilityRequest struct {
		domainID      string
		pageSize      int
		nextPageToken []byte
		parsedQuery   *parsedQuery
	}

	indexToArchive struct {
		primaryIndex            string
		primaryIndexValue       string
		secondaryIndex          string
		secondaryIndexTimestamp int64
	}
)

const (
	errEncodeVisibilityRecord       = "failed to encode visibility record"
	secondaryIndexKeyStartTimeout   = "startTimeout"
	secondaryIndexKeyCloseTimeout   = "closeTimeout"
	primaryIndexKeyWorkflowTypeName = "workflowTypeName"
	primaryIndexKeyWorkflowID       = "workflowID"
)

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on s3
func NewVisibilityArchiver(
	container *archiver.VisibilityBootstrapContainer,
	config *config.COSArchiver,
) (archiver.VisibilityArchiver, error) {
	return newVisibilityArchiver(container, config)
}

func newVisibilityArchiver(
	container *archiver.VisibilityBootstrapContainer,
	config *config.COSArchiver) (*visibilityArchiver, error) {
	return &visibilityArchiver{
		container:   container,
		cosCli:      connector.NewClient(context.TODO(), config),
		queryParser: NewQueryParser(),
	}, nil
}

// Archive TODO
func (v *visibilityArchiver) Archive(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.ArchiveVisibilityRequest,
	opts ...archiver.ArchiveOption,
) (err error) {
	scope := v.container.MetricsClient.Scope(metrics.VisibilityArchiverScope, metrics.DomainTag(request.DomainName))
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	sw := scope.StartTimer(metrics.CadenceLatency)
	logger := archiver.TagLoggerWithArchiveVisibilityRequestAndURI(v.container.Logger, request, URI.String())
	archiveFailReason := ""
	defer func() {
		sw.Stop()
		if err != nil {
			if persistence.IsTransientError(err) {
				scope.IncCounter(metrics.VisibilityArchiverArchiveTransientErrorCount)
				logger.Error(archiver.ArchiveTransientErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
			} else {
				scope.IncCounter(metrics.VisibilityArchiverArchiveNonRetryableErrorCount)
				logger.Error(archiver.ArchiveNonRetriableErrorMsg, tag.ArchivalArchiveFailReason(archiveFailReason), tag.Error(err))
				if featureCatalog.NonRetriableError != nil {
					err = featureCatalog.NonRetriableError()
				}
			}
		}
	}()

	if err := softValidateURI(URI); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidURI
		return err
	}

	if err := archiver.ValidateVisibilityArchivalRequest(request); err != nil {
		archiveFailReason = archiver.ErrReasonInvalidArchiveRequest
		return err
	}

	encodedVisibilityRecord, err := encode(request)
	if err != nil {
		archiveFailReason = errEncodeVisibilityRecord
		return err
	}
	indexes := createIndexesToArchive(request)
	// Upload archive to all indexes
	for _, element := range indexes {
		key := constructTimestampIndex(URI.Path(), request.DomainID, element.primaryIndex, element.primaryIndexValue,
			element.secondaryIndex, element.secondaryIndexTimestamp, request.RunID)
		if err := v.cosCli.Put(ctx, URI, key, encodedVisibilityRecord); err != nil {
			archiveFailReason = errWriteKey
			return err
		}
	}
	scope.IncCounter(metrics.VisibilityArchiveSuccessCount)
	return nil
}

func createIndexesToArchive(request *archiver.ArchiveVisibilityRequest) []indexToArchive {
	return []indexToArchive{
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyCloseTimeout, request.CloseTimestamp},
		{primaryIndexKeyWorkflowTypeName, request.WorkflowTypeName, secondaryIndexKeyStartTimeout, request.StartTimestamp},
		{primaryIndexKeyWorkflowID, request.WorkflowID, secondaryIndexKeyCloseTimeout, request.CloseTimestamp},
		{primaryIndexKeyWorkflowID, request.WorkflowID, secondaryIndexKeyStartTimeout, request.StartTimestamp},
	}
}

// Query TODO
func (v *visibilityArchiver) Query(
	ctx context.Context,
	URI archiver.URI,
	request *archiver.QueryVisibilityRequest,
) (*archiver.QueryVisibilityResponse, error) {
	if err := softValidateURI(URI); err != nil {
		return nil, &types.BadRequestError{Message: archiver.ErrInvalidURI.Error()}
	}

	if err := archiver.ValidateQueryRequest(request); err != nil {
		return nil, &types.BadRequestError{Message: archiver.ErrInvalidQueryVisibilityRequest.Error()}
	}

	parsedQuery, err := v.queryParser.Parse(request.Query)
	if err != nil {
		return nil, &types.BadRequestError{Message: err.Error()}
	}

	return v.query(ctx, URI, &queryVisibilityRequest{
		domainID:      request.DomainID,
		pageSize:      request.PageSize,
		nextPageToken: request.NextPageToken,
		parsedQuery:   parsedQuery,
	})
}

func (v *visibilityArchiver) query(
	ctx context.Context,
	URI archiver.URI,
	request *queryVisibilityRequest,
) (*archiver.QueryVisibilityResponse, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	var token *string
	if request.nextPageToken != nil {
		token = deserializeQueryVisibilityToken(request.nextPageToken)
	}
	primaryIndex := primaryIndexKeyWorkflowTypeName
	primaryIndexValue := request.parsedQuery.workflowTypeName
	if request.parsedQuery.workflowID != nil {
		primaryIndex = primaryIndexKeyWorkflowID
		primaryIndexValue = request.parsedQuery.workflowID
	}
	var prefix = constructVisibilitySearchPrefix(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue,
		secondaryIndexKeyCloseTimeout) + "/"
	if request.parsedQuery.closeTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue,
			secondaryIndexKeyCloseTimeout, *request.parsedQuery.closeTime, *request.parsedQuery.searchPrecision)
	}
	if request.parsedQuery.startTime != nil {
		prefix = constructTimeBasedSearchKey(URI.Path(), request.domainID, primaryIndex, *primaryIndexValue,
			secondaryIndexKeyStartTimeout, *request.parsedQuery.startTime, *request.parsedQuery.searchPrecision)
	}

	results, err := v.cosCli.ListObjects(ctx, URI, &cos.BucketGetOptions{
		Prefix:  prefix,
		Marker:  *token,
		MaxKeys: request.pageSize,
	})
	if err != nil {
		return nil, &types.BadRequestError{Message: err.Error()}
	}
	if len(results.Contents) == 0 {
		return &archiver.QueryVisibilityResponse{}, nil
	}

	response := &archiver.QueryVisibilityResponse{}
	if results.IsTruncated {
		response.NextPageToken = serializeQueryVisibilityToken(results.NextMarker)
	}
	for _, item := range results.Contents {
		encodedRecord, err := v.cosCli.Get(ctx, URI, item.Key)
		if err != nil {
			return nil, &types.InternalServiceError{Message: err.Error()}
		}

		record, err := decodeVisibilityRecord(encodedRecord)
		if err != nil {
			return nil, &types.InternalServiceError{Message: err.Error()}
		}
		response.Executions = append(response.Executions, convertToExecutionInfo(record))
	}
	return response, nil
}

// ValidateURI TODO
func (v *visibilityArchiver) ValidateURI(URI archiver.URI) error {
	err := softValidateURI(URI)
	if err != nil {
		return err
	}
	exist, err := v.cosCli.BucketExist(context.TODO(), URI)
	if exist {
		return nil
	}
	return err
}
