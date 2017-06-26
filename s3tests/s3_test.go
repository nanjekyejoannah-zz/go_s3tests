package s3test

import (

	"github.com/stretchr/testify/suite"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/service/s3"
)

type S3Suite struct {
    suite.Suite
}

func (suite *S3Suite) SetupTest() {
    
}

func (suite *S3Suite) TestBucketCreateReadDelete () {

	/* 
		Resource : bucket, method: create/delete
		Scenario : create and delete bucket. 
		Assertion: bucket exists after create and is gone after delete.
	*/

	assert := suite
	bucket := GetBucketName()

	err := CreateBucket(bucket)
	assert.Nil(err)

	bkts, err := ListBuckets()
	assert.Equal(true, Contains(bkts, bucket))

	
	err = DeleteBucket(bucket)

	err = DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func TestSuite(t *testing.T) {

    suite.Run(t, new(S3Suite))

}

func (suite *S3Suite) TearDownTest() {
	assert := suite

	bucketName := GetBucketName()
	err := DeleteBucket(bucketName)
	assert.NotNil(err)   
}