package s3test

import (

	"github.com/stretchr/testify/suite"
	"github.com/aws/aws-sdk-go/aws/awserr"

	. "../Utilities"
)

type HeadSuite struct {
    suite.Suite
}

func (suite *HeadSuite) TestObjectAclCreateContentlengthNone() {

	/* 
		Resource : bucket, method: acls
		Scenario :set w/no content length. 
		Assertion: suceeds
	*/

	assert := suite
	conLength := map[string]string{"Content-Length": "",}
	acl := map[string]string{"ACL": "public-read",}
	content := "bar"

	bucket := GetBucketName()
	key := "key1"
	err := CreateBucket(svc, bucket)

	err = SetupObjectWithHeader(svc, bucket, key, content, conLength)
	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *HeadSuite) TestBucketPutCanned_acl() {

	/* 
		Resource : bucket, method: put
		Scenario :set w/invalid permission. 
		Assertion: fails
	*/

	assert := suite
	cannedAcl := map[string]string{"x-amz-acl": "public-ready",}
	acl := map[string]string{"ACL": "public-read",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, cannedAcl)
	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "AccessDenied")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadExpectMismatch() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/expect 200. 
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": "200",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *HeadSuite) TestBucketCreateBadExpectEmpty() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/expect empty. 
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": " ",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *HeadSuite) TestBucketCreateBadExpectUnreadable() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/expect nongraphic. 
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": "\x07",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *HeadSuite) TestBucketCreateBadContentLengthEmpty() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/empty content length. 
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": " ",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "XAmzContentSHA256Mismatch")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadContentlengthNegative() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/negative content length. 
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": "-1",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "XAmzContentSHA256Mismatch")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadContentlengthNone() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/no content length. 
		Assertion: suceeds
	*/

	assert := suite
	acl := map[string]string{"Content-Length": "",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *HeadSuite) TestBucket_CreateBadContentlengthUnreadable() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/unreadable content length. 
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": "\x07",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "XAmzContentSHA256Mismatch")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadAuthorizationUnreadable() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/non-graphic authorization. 
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": "\x07",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "AccessDenied")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadAuthorizationEmpty() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/empty authorization. 
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": " ",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "AccessDenied")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *HeadSuite) TestBucketCreateBadAuthorizationNone() {

	/* 
		Resource : bucket, method: put
		Scenario :create w/no authorization. 
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": "",}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "AccessDenied")
			assert.Equal(awsErr.Message(), "")
		}
	}
}








