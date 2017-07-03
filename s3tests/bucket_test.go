package s3test
import (

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	
	 . "../Utilities"
)

func (suite *S3Suite) TestBucketCreateReadDelete () {

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
	objects := map[string]string{ "key1": "echo",}

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

func  (suite *S3Suite) TestBucketListDistinct() {

	/* 
		Resource : object, method: list
		Scenario : bucket not empty 
		Assertion: distinct buckets have different contents.
	*/

	assert := suite
	bucket1 := GetBucketName()
	bucket2 := GetBucketName()
	objects1 := map[string]string{ "key1": "Hello",}
	objects2 := map[string]string{ "key2": "Manze",}

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