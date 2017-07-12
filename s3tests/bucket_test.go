package s3test

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	"crypto/md5"
	"encoding/base64"
	"strings"

	. "../Utilities"
)

func (suite *S3Suite) TestBucketCreateReadDelete() {

	/*
		Resource : bucket, method: create/delete
		Scenario : create and delete bucket.
		Assertion: bucket exists after create and is gone after delete.
	*/

	assert := suite
	bucket := GetBucketName()

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	bkts, err := ListBuckets(svc)
	assert.Equal(true, Contains(bkts, bucket))

	err = DeleteBucket(svc, bucket)

	//ensure it doesnt exist
	err = DeleteBucket(svc, bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *S3Suite) TestBucketDeleteNotExist() {

	/*
		Resource : bucket, method: delete
		Scenario : non existant bucket
		Assertion: fails NoSuchBucket.
	*/

	assert := suite
	bucket := GetBucketName()

	err := DeleteBucket(svc, bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}

}

func (suite *S3Suite) TestBucketDeleteNotEmpty() {

	/*
		Resource : bucket, method: delete
		Scenario : bucket not empty
		Assertion: fails BucketNotEmpty.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{"key1": "echo"}

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	err = CreateObjects(svc, bucket, objects)

	err = DeleteBucket(svc, bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "BucketNotEmpty")
			assert.Equal(awsErr.Message(), "")
		}
	}

}

func (suite *S3Suite) TestBucketListEmpty() {

	/*
		Resource : object, method: list
		Scenario : bucket not empty
		Assertion: empty buckets return no contents.
	*/

	assert := suite
	bucket := GetBucketName()
	var empty_list []*s3.Object

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	resp, err := GetObjects(svc, bucket)
	assert.Nil(err)
	assert.Equal(empty_list, resp.Contents)
}

func (suite *S3Suite) TestBucketListDistinct() {

	/*
		Resource : object, method: list
		Scenario : bucket not empty
		Assertion: distinct buckets have different contents.
	*/

	assert := suite
	bucket1 := GetBucketName()
	bucket2 := GetBucketName()
	objects1 := map[string]string{"key1": "Hello"}
	objects2 := map[string]string{"key2": "Manze"}

	err := CreateBucket(svc, bucket1)
	err = CreateBucket(svc, bucket2)
	assert.Nil(err)

	err = CreateObjects(svc, bucket1, objects1)
	err = CreateObjects(svc, bucket2, objects2)

	obj1, _ := GetObject(svc, bucket1, "key1")
	obj2, _ := GetObject(svc, bucket2, "key2")

	assert.Equal(obj1, "Hello")
	assert.Equal(obj2, "Manze")

}

func (suite *S3Suite) TestObjectAclCreateContentlengthNone() {

	/*
		Resource : bucket, method: acls
		Scenario :set w/no content length.
		Assertion: suceeds
	*/

	assert := suite
	conLength := map[string]string{"Content-Length": ""}
	acl := map[string]string{"ACL": "public-read"}
	content := "bar"

	bucket := GetBucketName()
	key := "key1"
	err := CreateBucket(svc, bucket)

	err = SetupObjectWithHeader(svc, bucket, key, content, conLength)
	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *S3Suite) TestBucketPutCanned_acl() {

	/*
		Resource : bucket, method: put
		Scenario :set w/invalid permission.
		Assertion: fails
	*/

	assert := suite
	cannedAcl := map[string]string{"x-amz-acl": "public-ready"}
	acl := map[string]string{"ACL": "public-read"}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, cannedAcl)
	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "AccessDenied")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *S3Suite) TestBucketCreateBadExpectMismatch() {

	/*
		Resource : bucket, method: put
		Scenario :create w/expect 200.
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": "200"}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *S3Suite) TestBucketCreateBadExpectEmpty() {

	/*
		Resource : bucket, method: put
		Scenario :create w/expect empty.
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": " "}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *S3Suite) TestBucketCreateBadExpectUnreadable() {

	/*
		Resource : bucket, method: put
		Scenario :create w/expect nongraphic.
		Assertion: garbage, but S3 succeeds!
	*/

	assert := suite
	acl := map[string]string{"Expect": "\x07"}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *S3Suite) TestBucketCreateBadContentLengthEmpty() {

	/*
		Resource : bucket, method: put
		Scenario :create w/empty content length.
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": " "}

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

func (suite *S3Suite) TestBucketCreateBadContentlengthNegative() {

	/*
		Resource : bucket, method: put
		Scenario :create w/negative content length.
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": "-1"}

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

func (suite *S3Suite) TestBucketCreateBadContentlengthNone() {

	/*
		Resource : bucket, method: put
		Scenario :create w/no content length.
		Assertion: suceeds
	*/

	assert := suite
	acl := map[string]string{"Content-Length": ""}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	err = CreateBucketWithHeader(svc, bucket, acl)
	assert.Nil(err)
}

func (suite *S3Suite) TestBucket_CreateBadContentlengthUnreadable() {

	/*
		Resource : bucket, method: put
		Scenario :create w/unreadable content length.
		Assertion: fails
	*/

	assert := suite
	acl := map[string]string{"Content-Length": "\x07"}

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

func (suite *S3Suite) TestBucketCreateBadAuthorizationUnreadable() {

	/*
		Resource : bucket, method: put
		Scenario :create w/non-graphic authorization.
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": "\x07"}

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

func (suite *S3Suite) TestBucketCreateBadAuthorizationEmpty() {

	/*
		Resource : bucket, method: put
		Scenario :create w/empty authorization.
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": " "}

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

func (suite *S3Suite) TestBucketCreateBadAuthorizationNone() {

	/*
		Resource : bucket, method: put
		Scenario :create w/no authorization.
		Assertion: expected to fail..but suceeded
	*/

	assert := suite
	acl := map[string]string{"Authorization": ""}

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

func (suite *S3Suite) TestLifecycleGetNoLifecycle() {

	/*
		Resource : bucket, method: get
		Scenario : get lifecycle config that has not been set.
		Assertion: fails
	*/

	assert := suite
	//acl := map[string]string{"Authorization": ""}

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)

	_, err = GetLifecycle(svc, bucket)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchLifecycleConfiguration")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *S3Suite) TestLifecycleInvalidMD5() {

	/*
		Resource : bucket, method: get
		Scenario : set lifecycle config with invalid md5.
		Assertion: fails
	*/

	assert := suite

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)


	content := strings.NewReader("Enabled")
	h := md5.New()
	content.WriteTo(h)
	sum := h.Sum(nil)
	b := make([]byte, base64.StdEncoding.EncodedLen(len(sum)))
	base64.StdEncoding.Encode(b,sum)

	md5 := string(b)

	_, err = SetLifecycle(svc, bucket, "rule1", "Enabled", md5)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NotImplemented")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func (suite *S3Suite) TestLifecycleInvalidStatus() {

	/*
		Resource : bucket, method: get
		Scenario : invalid status in lifecycle rule.
		Assertion: fails
	*/

	assert := suite

	bucket := GetBucketName()
	err := CreateBucket(svc, bucket)


	content := strings.NewReader("Enabled")
	h := md5.New()
	content.WriteTo(h)
	sum := h.Sum(nil)
	b := make([]byte, base64.StdEncoding.EncodedLen(len(sum)))
	base64.StdEncoding.Encode(b,sum)

	md5 := string(b)

	_, err = SetLifecycle(svc, bucket, "rule1", "enabled", md5)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NotImplemented")
			assert.Equal(awsErr.Message(), "")
		}
	}

	_, err = SetLifecycle(svc, bucket, "rule1", "disabled", md5)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NotImplemented")
			assert.Equal(awsErr.Message(), "")
		}
	}

	_, err = SetLifecycle(svc, bucket, "rule1", "invalid", md5)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NotImplemented")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

