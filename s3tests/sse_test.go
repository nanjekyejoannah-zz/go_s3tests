package s3test

import (

	"github.com/stretchr/testify/suite"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"

	. "../Utilities"
)

type SSESuite struct {
    suite.Suite
}

func (suite *SSESuite) TestEncryptedTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestEncryptedTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1KB
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestEncryptedTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1MB
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestEncryptedTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSbarbTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSbarbTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSbarbTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSbarbTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 mega byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 kilobyte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *SSESuite) TestSSEKMSPresent() {

	/* 
		Resource : object, method: put
		Scenario : write encrypted with SSE-KMS and read without SSE-KMS
		Assertion: success.
	*/
	assert := suite
	bucket := GetBucketName()

	err := CreateBucket(svc, bucket)

	err = WriteSSEKMSkeyId(svc, bucket, "kay1", "test", []string{"AES256"}, "barbican_key_id")
	data, err := GetObject(svc, bucket, "kay1")
	assert.Nil(err)
	assert.Equal("test", data)
}

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

