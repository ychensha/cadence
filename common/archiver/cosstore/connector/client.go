package connector

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/config"
	"go.uber.org/multierr"
)

// Client TODO
type Client interface {
	Put(ctx context.Context, URI archiver.URI, name string, file []byte) error
	Get(ctx context.Context, URI archiver.URI, name string) ([]byte, error)
	Exist(ctx context.Context, URI archiver.URI, name string) (bool, error)
	BucketExist(ctx context.Context, URI archiver.URI) (bool, error)
	ListObjects(ctx context.Context, URI archiver.URI, options *cos.BucketGetOptions) (*cos.BucketGetResult, error)
}

type client struct {
	cosCli *cos.Client
}

func ensureBucketURL(URI archiver.URI, cosCli *cos.Client) {
	if cosCli.BaseURL.BucketURL == nil {
		u, _ := url.Parse(URI.String())
		cosCli.BaseURL = &cos.BaseURL{BucketURL: u}
	}
}

func getFullName(URI archiver.URI, name string) string {
	return URI.Path()[1:] + "/" + name
}

// Put TODO
func (c *client) Put(ctx context.Context, URI archiver.URI, name string, data []byte) error {
	ensureBucketURL(URI, c.cosCli)
	_, err := c.cosCli.Object.Put(ctx, getFullName(URI, name), bytes.NewReader(data), nil)
	return err
}

// Get TODO
func (c *client) Get(ctx context.Context, URI archiver.URI, name string) ([]byte, error) {
	ensureBucketURL(URI, c.cosCli)
	rsp, err := c.cosCli.Object.Get(ctx, getFullName(URI, name), nil)
	defer func() {
		if ierr := rsp.Body.Close(); ierr != nil {
			err = multierr.Append(err, ierr)
		}
	}()

	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Exist TODO
func (c *client) Exist(ctx context.Context, URI archiver.URI, name string) (bool, error) {
	ensureBucketURL(URI, c.cosCli)
	return c.cosCli.Object.IsExist(ctx, getFullName(URI, name))
}

// BucketExist TODO
func (c *client) BucketExist(ctx context.Context, URI archiver.URI) (bool, error) {
	ensureBucketURL(URI, c.cosCli)
	return c.cosCli.Bucket.IsExist(ctx)
}

// ListObjects TODO
func (c *client) ListObjects(ctx context.Context, URI archiver.URI, opts *cos.BucketGetOptions) (*cos.BucketGetResult, error) {
	ensureBucketURL(URI, c.cosCli)
	rsp, _, err := c.cosCli.Bucket.Get(ctx, opts)
	return rsp, err
}

// NewClient TODO
func NewClient(ctx context.Context, config *config.COSArchiver) Client {
	return &client{
		cosCli: cos.NewClient(nil, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:  config.SecretID,
				SecretKey: config.SecretKey,
			},
		})}
}
