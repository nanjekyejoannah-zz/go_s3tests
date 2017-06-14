package s3tests

import (
	assert "github.com/stretchr/testify/require"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

//.............Bucket Operations.............................................................

func TestBucketCreateReadDelete(t *testing.T) {

	//should be able to create, delete and list buckets.

	assert := assert.New(t)
	bucket := "bucket1"

	err := CreateBucket(bucket)
	assert.Nil(err)

	bkts, err := ListBuckets()
	assert.Nil(err)
	assert.Equal(true, SliceContains(bkts, bucket))

	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestBucketDeleteNonExistant(t *testing.T) {

	// should not delete non existant bucket

	assert := assert.New(t)
	bucket := "bucket9"

	err := DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
			//assert.Equal(awsErr.Error().Code, 409)
		}
	}

}

func TestBucketDeleteNotEmpty(t *testing.T) {

	// should not delete a bucket with contents

	assert := assert.New(t)
	bucket := "bucket1"

	err := CreateBucket(bucket)
	assert.Nil(err)

	err = PutObjectToBucket(bucket, "key1", "Hello")

	err = DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "BucketNotEmpty")
			assert.Equal(awsErr.Message(), "")
			//assert.Equal(awsErr.Error().Code, 409)
		}
	}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListEmpty(t *testing.T) {

	//empty bucket returns no contents

	assert := assert.New(t)
	bucket := "bucket1"
	var empty_list []*s3.Object

	err := CreateBucket(bucket)
	assert.Nil(err)

	objects, err := ListObjects(bucket)
	assert.Nil(err)
	assert.Equal(empty_list, objects)

	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestBucketListDistinct(t *testing.T) {

	// distinct buckets should have distinct contents

	assert := assert.New(t)
	bucket1 := "bucket1"
	bucket2 := "bucket2"

	err := CreateBucket(bucket1)
	err = CreateBucket(bucket2)
	assert.Nil(err)

	err = PutObjectToBucket(bucket1, "key1", "Hello")
	err = PutObjectToBucket(bucket2, "key2", "Manze")

	obj1, _ := GetObject(bucket1, "key1")
	obj2, _ := GetObject(bucket2, "key2")

	assert.Equal(obj1, "Hello")
	assert.Equal(obj2, "Manze")

	err = DeleteObjects(bucket1)
	err = DeleteObjects(bucket2)
	err = DeleteBucket(bucket1)
	err = DeleteBucket(bucket2)
	assert.Nil(err)

}

func TestBucketListMany(t *testing.T) {

	// pagination w/max_keys=2, no marker

	assert := assert.New(t)
	bucket := "bucket11"
	expected_keys := []string{"key1", "key2"}
	//expected_key := []string{"key1"}

	err := CreateBucket(bucket)
	assert.Nil(err)

	err = PutObjectToBucket(bucket, "key1", "echo")
	err = PutObjectToBucket(bucket, "key2", "lima")
	err = PutObjectToBucket(bucket, "key3", "golf")
	err = PutObjectToBucket(bucket, "key4", "alpha")
	assert.Nil(err)

	var keys []string
	keys, err = GetKeys(bucket)
	assert.Nil(err)
	assert.Equal(4, len(keys))

	keys, err = GetKeysWithMaxKeys(bucket, 2)
	assert.Nil(err)
	assert.Equal(2, len(keys))
	assert.Equal(expected_keys, keys)

	// keys, err = GetKeysWithMaxKeysAndMarker(bucket, 2, expected_keys[0] )
	// assert.Nil(err)
	// assert.Equal(1, len(keys))
	// assert.Equal(expected_key, keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

//................................................Object Operations...............................................................................

func TestObjectReadNotExist(t *testing.T) {

	// Reading content that was never written should fail

	assert := assert.New(t)
	bucket1 := "bucket1"

	err := CreateBucket(bucket1)
	assert.Nil(err)

	_, err = GetObject(bucket1, "key6")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchKey")
			assert.Equal(awsErr.Message(), "")
			//assert.Equal(awsErr.Error(), 404)

		}
	}

	err = DeleteBucket(bucket1)
	assert.Nil(err)

}

func TestObjectWriteToNonExistBucket(t *testing.T) {

	// writing to a non existant bucket should fail

	assert := assert.New(t)
	non_exixtant_bucket := "bucketz"

	_, err := GetObject(non_exixtant_bucket, "key6")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}

	}

}

func TestObjectWriteReadUpdateReadDelete(t *testing.T) {

	// Reading content that was never written should fail
	assert := assert.New(t)
	bucket := "bucket1"
	key := "key1"

	err := CreateBucket(bucket)
	assert.Nil(err)

	// Write object
	err = PutObjectToBucket(bucket, key, "hello")
	assert.Nil(err)

	// Read object
	result, _ := GetObject(bucket, key)
	assert.Equal(result, "hello")

	//Update object
	err = PutObjectToBucket(bucket, key, "Come on !!")
	assert.Nil(err)

	// Read object again
	result, _ = GetObject(bucket, key)
	assert.Equal(result, "Come on !!")

	err = DeleteObjects(bucket)
	assert.Nil(err)

	// If object was well deleted, there shouldn't be an error at this point
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectDeleteAll(t *testing.T) {

	// Reading content that was never written should fail
	assert := assert.New(t)

	var empty_list []*s3.Object
	bucket := "bucket5"
	key := "key5"
	key1 := "key55"

	err := CreateBucket(bucket)
	assert.Nil(err)

	err = PutObjectToBucket(bucket, key, "hello")
	err = PutObjectToBucket(bucket, key1, "foo")
	assert.Nil(err)
	objects, err := ListObjects(bucket)
	assert.Nil(err)
	assert.Equal(2, len(objects))

	err = DeleteObjects(bucket)
	assert.Nil(err)

	objects, err = ListObjects(bucket)
	assert.Nil(err)
	assert.Equal(empty_list, objects)

	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectCopyBucketNotFound(t *testing.T) {

	// copy from non-existent bucket

	assert := assert.New(t)
	bucket := "bucket4"
	item := "key1"
	other := "bucket2"

	source := bucket + "/" + item

	err := CreateBucket(bucket)
	assert.Nil(err)

	// Write object
	err = PutObjectToBucket(bucket, item, "hello")
	assert.Nil(err)

	err = CopyObject(other, source, item)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}

	}

	err = DeleteObjects(bucket)
	assert.Nil(err)

	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestObjectCopyKeyNotFound(t *testing.T) {

	assert := assert.New(t)
	bucket := "bucket4"
	item := "key1"
	other := "bucket2"

	source := bucket + "/" + item

	err := CreateBucket(bucket)
	err = CreateBucket(other)
	assert.Nil(err)

	err = CopyObject(other, source, item)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchKey")
			assert.Equal(awsErr.Message(), "")
		}

	}

	err = DeleteObjects(bucket)
	err = DeleteObjects(other)
	assert.Nil(err)

	err = DeleteBucket(bucket)
	err = DeleteBucket(other)
	assert.Nil(err)

}

