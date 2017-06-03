import (
	"github.com/aws/aws-sdk-go/service/s3"
	import "github.com/aws/aws-sdk-go/aws/awserr"

	"testing"
	assert "github.com/stretchr/testify/require"

	"helpers/utils"
)

func TestObjectCRUD(t *testing.T) {
	/*
	
	# Related python test
		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1110
	
	# Goal
	 To test if we can write , read , update and delete an object.

	## Test Process

		I created a new bucket
		Wrote some data there.
		Made an assertion to see if the data I wrote exists and can be read.

		## Testing update

			There is no update method in the Go SDK. 
			I decided to write new data in the same bucket and using an existing key. 
			This should overwrite the current data.
	        I made an assertion checking if the new data has been writen.

		## testing object Delete
			I simply call a delete method from the SDK with a bucket name and key.
			And make an assertion to ensure there is no data after delete.
	*/
	assert := assert.New(t)
	bucketname := "bucket1"
	key : "key1"
	word1 := "cheers"
	word2 :+ "cheers again"


	utils.CreateBucket(bucketname)
	// write and read
	utils.SetStringObject(bucket, key, word1)
	read = utils.GetStringObject(bucket, key)
	assert.Equal(read, word1) 

	// update
	utils.SetStringObject(bucket, key, word2)
	read = utils.GetStringObject(bucket, key)
	assert.Equal(read, word2) 

	//delete
	utils.DeleteStringObject(bucket, key)
	assert.Equal(read, "")

	err := utils.DeleteBucket(bucketname)

}

func TestMultipleObjectDelete(t *testing.T) {
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1035
	
	# Goal

		To test if we can delete many objects from a bucket.

	# Test Process
		Create one bucket
		write some objects to it
		Before delete, I make as assertion on the number of objects currently sitting there.
		perform a delete for all the objects in the bucket.
		Make an assertion to ensure the bucket is empty.
		However I perform another delete of objects from the same bucket.
		This should work without errors due idempotency.
		Finally do clean up by deleting a bucket I created for testing purposes.
	*/
	assert := assert.New(t)
	bucket := "bucket0"
	utils.CreateBucket(bucket)
	obj_count := len(utils.ListStringObjects(bucket))
	assert.Equal(len(obj_count, 0)
	utils.WriteObject(bucket, "key0", "Hello")
	utils.WriteObject(bucket, "key1", "world")
	utils.WriteObject(bucket, "key2", "again")
	assert.Equal(obj_count, 3)
	err := utils.DeleteStringObjects(bucket)
	assert.Equalerr, nil)
	assert.Equal(len(obj_count, 0)
	err = utils.DeleteStringObjects(bucket)
	assert.Equal(len(obj_count, 0)
	err := utils.DeleteBucket(bucket)
}

func TestDeleteNonExistantBucket(t *testing.T) {
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L887
	
	# Goal

		Deleting a non existant bucket should return an error.

	# Test Process
		I try to delete a bucket that was not created.
		Make an assertion to ensure an error was returned.
		I make more assertions to ensure the returned error details match.
	*/

	assert := assert.New(t)
	non_existant_bucket := "bucket0"

	err := utils.DeleteBucket(non_existant_bucket)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

		assert.Equal(awsErr.Code(), "NoSuchBucket")
		assert.Equal(awsErr.Message(), "Not Found")
		assert.Equal(awsErr.Error(), 404)

		}

	}
	
}

func TestDeleteObjectBucketGone(t *testing.T) {
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L2679
		Requires that we can not delete a key of a non existant/gone/deleted bucket.
	
	# Goal

		Deleting an object from an non existant/gone/deleted  bucket should fail.
		Note with the GoSDK there is no method for deleting just a key
		Instead we delete the associated object querying by its key.

	# Test Process
		I create a bucket.
		Delete the bucket
		Make an assertion it got deleted without error
		The I try to delete an object from the deleted bucket
		I Make an assertion to ensure an error was returned.
		I make more assertions to ensure the returned error details match.
	*/
	assert := assert.New(t)

	bucket := "bucket0"
	key    := "Key"
	utils.CreateBucket(bucket)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)
    err := utils.DeleteStringObject(bucket, key)
    assert.NotNil(err)
    if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

		assert.Equal(awsErr.Code(), "NoSuchBucket")
		assert.Equal(awsErr.Message(), "Not Found")
		assert.Equal(awsErr.Error(), 404)

		}

	}
}

func TestPutObjectIfnonmatchOverwriteExistedFailed(t *testing.T) {
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L2607
	
	# Goal

		Updating an object with NoIfNoneMatch header should fail.

	# Test Process
		I create a bucket and an object.
		And then try to updatit with header.
		Make an assertion to ensure an error was returned.
		I make more assertions to ensure the returned error details match.
		I also make an assertion to ensure the old data still stands.
	*/

	assert := assert.New(t)

	bucket := "bucket0"
	key    := "Key"
	utils.CreateBucket(bucket)

	err := utils.SetStringObject(bucket, key, "Echo Lima Golf")
	assert.Nil(err)
	assert.Equal(utils.GetStringObject(bucket, key), "Echo Lima Golf")

	err := utils.SetStringObjectWithNoIfNoneMatch(bucket, key, "Roger")
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

		assert.Equal(awsErr.Code(), "PreconditionFailed")
		assert.Equal(awsErr.Message(), "PreconditionFailed")
		assert.Equal(awsErr.Error(), 412)

		}

	}

	old_data = utils.GetStringObject(bucket, key) 
	assert.Equal(old_data, "Echo Lima Golf")

	err := utils.DeleteStringObject(bucket, key)
	assert.Nil(err)
	_  := utils.DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListDelimiterNotExist(t *testing.T) 
{
	key_names := []string{"echo", "alpha", "golf"}
	bucket := "bucket0"
	key    := "Key"
	
	utils.CreateBucket(bucket)
	err := utils.SetStringObject(bucket, key, "echo")
	err := utils.SetStringObject(bucket, key, "alpha")
	err := utils.SetStringObject(bucket, key, "golf")
	assert.NotNil(err)


}

