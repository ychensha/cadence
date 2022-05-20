package connector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/cosstore/connector/mocks"
)

func Test_client_Put(t *testing.T) {
	ctx := context.Background()
	cli := &mocks.Client{}
	URI, err := archiver.NewURI("https://your-bucket.cos.ap-nanjing.myqcloud.com/cadence/test")
	require.Nil(t, err)
	data := []byte("test")
	name := "myfile.history"
	cli.On("Put", ctx, URI, name, data).Return(nil).Times(1)

	err = cli.Put(ctx, URI, name, data)
	require.Nil(t, err)
}

func Test_client_Get(t *testing.T) {
	ctx := context.Background()
	cli := &mocks.Client{}
	URI, err := archiver.NewURI("https://your-bucket.cos.ap-nanjing.myqcloud.com/cadence/test")
	require.Nil(t, err)
	data := []byte("test")
	name := "myfile.history"
	cli.On("Get", ctx, URI, name).Return(data, nil).Times(1)
	_, err = cli.Get(context.TODO(), URI, name)
	require.Nil(t, err)
}

func Test_client_Exist(t *testing.T) {
	ctx := context.Background()
	cli := &mocks.Client{}
	URI, err := archiver.NewURI("https://your-bucket.cos.ap-nanjing.myqcloud.com/cadence/test")
	require.Nil(t, err)
	name := "myfile.history"
	cli.On("Exist", ctx, URI, name).Return(true, nil).Times(1)
	_, err = cli.Exist(context.TODO(), URI, name)
	require.Nil(t, err)
}

func Test_client_BucketExist(t *testing.T) {
	ctx := context.Background()
	cli := &mocks.Client{}
	URI, err := archiver.NewURI("https://your-bucket.cos.ap-nanjing.myqcloud.com/cadence/test")
	require.Nil(t, err)
	cli.On("BucketExist", ctx, URI).Return(true, nil).Times(1)
	_, err = cli.BucketExist(context.TODO(), URI)
	require.Nil(t, err)
}
