package s3test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

func TestComputeSSEKeys(t *testing.T) {
	s := s3.New(unit.Session)
	req, _ := s.CopyObjectRequest(&s3.CopyObjectInput{
		Bucket:                   aws.String("bucket"),
		CopySource:               aws.String("bucket/source"),
		Key:                      aws.String("dest"),
		SSECustomerKey:           aws.String("key"),
		CopySourceSSECustomerKey: aws.String("key"),
	})
	err := req.Build()

	assert.NoError(t, err)
	assert.Equal(t, "a2V5", req.HTTPRequest.Header.Get("x-amz-server-side-encryption-customer-key"))
	assert.Equal(t, "a2V5", req.HTTPRequest.Header.Get("x-amz-copy-source-server-side-encryption-customer-key"))
	assert.Equal(t, "PG4LipwVIkqCKLmpjKFTHQ==", req.HTTPRequest.Header.Get("x-amz-server-side-encryption-customer-key-md5"))
	assert.Equal(t, "PG4LipwVIkqCKLmpjKFTHQ==", req.HTTPRequest.Header.Get("x-amz-copy-source-server-side-encryption-customer-key-md5"))
}

func TestComputeSSEKeysShortcircuit(t *testing.T) {
	s := s3.New(unit.Session)
	req, _ := s.CopyObjectRequest(&s3.CopyObjectInput{
		Bucket:                      aws.String("bucket"),
		CopySource:                  aws.String("bucket/source"),
		Key:                         aws.String("dest"),
		SSECustomerKey:              aws.String("key"),
		CopySourceSSECustomerKey:    aws.String("key"),
		SSECustomerKeyMD5:           aws.String("MD5"),
		CopySourceSSECustomerKeyMD5: aws.String("MD5"),
	})
	err := req.Build()

	assert.NoError(t, err)
	assert.Equal(t, "a2V5", req.HTTPRequest.Header.Get("x-amz-server-side-encryption-customer-key"))
	assert.Equal(t, "a2V5", req.HTTPRequest.Header.Get("x-amz-copy-source-server-side-encryption-customer-key"))
	assert.Equal(t, "MD5", req.HTTPRequest.Header.Get("x-amz-server-side-encryption-customer-key-md5"))
	assert.Equal(t, "MD5", req.HTTPRequest.Header.Get("x-amz-copy-source-server-side-encryption-customer-key-md5"))
}
