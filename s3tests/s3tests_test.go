import (
	"github.com/aws/aws-sdk-go/service/s3"
	import "github.com/aws/aws-sdk-go/aws/awserr"

	"testing"
	assert "github.com/stretchr/testify/require"

	"helpers/utils"
)

func TestObjectWriteReadUpdateReadDelete(t *testing.T) {
	/*
	
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1110
	
	# Goal
	 To test if we can write , read , update and delete an object.

	## Test Process

		## Testing Object Read and write

			I created a new bucket
			Wrote some objects there.
			Made an assertion to see if the data I wrote exists and can be read.

		## Testing object update

			There is no update method in the Go SDK. 
			I decided to write new data in the same bucket using an existing key. 
			This should overwrite the current data.
	        I made an assertion checking if the new data has been over writen.

		## testing object Delete

			I simply call a delete method from the SDK with a bucket name and key.
			And make an assertion to ensure there is no data after delete.
	*/
	assert := assert.New(t)
	bucketname := "bucket1"
	key : "key1"
	word1 := "cheers"
	word2 :+ "cheers again"


	err = utils.CreateBucket(bucketname)
	assert.Nil(err)
	// write and read
	err = utils.SetStringObject(bucket, key, word1)
	assert.Nil(err)
	read = utils.GetStringObject(bucket, key)
	assert.Equal(read, word1) 

	// update
	err = utils.SetStringObject(bucket, key, word2)
	assert.Nil(err)
	read = utils.GetStringObject(bucket, key)
	assert.Equal(read, word2) 

	//delete
	err = utils.DeleteStringObject(bucket, key)
	assert.Nil(err)
	assert.Equal(read, "")

	err := utils.DeleteBucket(bucketname)
	assert.Nil(err)

}

func TestMultipleObjectDelete(t *testing.T) {
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1035
	
	# Goal

		To test if we can delete many objects from a bucket.

	# Test Process

		I Created one bucket
		wrote some objects to it
		Before delete, I make an assertion on the number of objects currently there.
		I performed a delete for all the objects in the bucket.
		Make an assertion to ensure the bucket is empty.
		However I perform another delete of objects from the same bucket.
		This should work without errors due idempotency.
		Finally do clean up by deleting a bucket I created for testing purposes.
	*/
	assert := assert.New(t)
	bucket := "bucket0"

	err = utils.CreateBucket(bucket)
	assert.Nil(err)

	obj_count := len(utils.ListStringObjects(bucket))
	assert.Equal(len(obj_count, 0)

	err = utils.WriteObject(bucket, "key0", "Hello")
	err = utils.WriteObject(bucket, "key1", "world")
	err = utils.WriteObject(bucket, "key2", "again")
	assert.Equal(obj_count, 3)

	err := utils.DeleteStringObjects(bucket)
	assert.Equal(err, nil)

	assert.Equal(len(obj_count, 0)
	err = utils.DeleteStringObjects(bucket)

	assert.Equal(len(obj_count, 0)

	err = utils.DeleteStringObject(bucket, key)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)
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
		I make more assertions to ensure the returned error details match the exception.
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

	# Test Process
		I create a bucket.
		I then Delete the bucket
		Make an assertion it got deleted without error
		The I try to delete an object from the deleted bucket
		I Make an assertion to ensure an error was returned.
		I make more assertions to ensure the returned error details match.
	*/
	assert := assert.New(t)

	bucket := "bucket0"
	key    := "Key"
	err = utils.CreateBucket(bucket)
	err = utils.DeleteBucket(bucket)
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
		And then try to update it with a header.
		I then Make an assertion to ensure an error was returned.
		I make more assertions to ensure the returned error details match the exception.
		I also make an assertion to ensure the old data still stands.
	*/

	assert := assert.New(t)

	bucket := "bucket0"
	key    := "Key"
	err = utils.CreateBucket(bucket)
	assert.Nil(err)

	err = utils.SetStringObject(bucket, key, "Echo Lima Golf")
	assert.Nil(err)
	assert.Equal(utils.GetStringObject(bucket, key), "Echo Lima Golf")

	err := utils.SetStringObjectWithNoIfNoneMatch(bucket, key, "Roger")
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

		assert.Equal(awsErr.Code(), "PreconditionFailed")
		assert.Equal(awsErr.Message(), "Precondition Failed")
		assert.Equal(awsErr.Error(), 412)

		}

	}

	old_data = utils.GetStringObject(bucket, key) 
	assert.Equal(old_data, "Echo Lima Golf")

	err := utils.DeleteStringObject(bucket, key)
	assert.Nil(err)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListDelimiterNotExist(t *testing.T) 
{
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L401
	
	# Goal

		The goal is to ensure that an unused delimeter is not found.

	# Test Process
		I create a bucket and some objects.
		I try to group my keys with delimiter "/"
		I Make an assertion to ensure that the returned keys 
		do not contain a delimeter that wasnt used.
	*/

	key_names := []string{"echo", "alpha", "golf"}
	bucket := "bucket0"
	key    := "Key"
	
	err = utils.CreateBucket(bucket)
	err := utils.SetStringObject(bucket, "key1", "echo")
	err := utils.SetStringObject(bucket, "key2", "alpha")
	err := utils.SetStringObject(bucket, "key3", "golf")
	assert.NotNil(err)

	keys, prefixes = util.ListkeyswithDelimeter(bucket, "/")
	 keynames []string
	for value := range len(keys) {
		for i := 0; i < len(value); i++ {
	        keynames = Extend(*value.Name, i)
	    }
	}
	assert.Equal(prefixes, [])
	assert.Equal(keynames, ["key1","key2","key3"])

	err := utils.DeleteStringObject(bucket, key)
	assert.Nil(err)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)

}

func TestPostObjectUploadSizelimitExceeded(t *testing.T) 
{
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L2196
	
	# Goal

		The goal is to ensure that a file that execeeds the upload file limit is not uploaded.
		According to aws specification a single put operation should not exceed 5GB and likewise
		multiple part uploads should not exceed 5TB.

	# Test Process
		I create a bucket
		I try to post a file that is bigger than 5GB
		I Make an assertion to ensure that an error is returned.
		I make more assertions to ensure the returned error details match the exception.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	err = utils.CreateBucket(bucket)
	assert.Nil(err)

	r, err := utils.UploadFile(bucket, "AWIT.jpg") // this a file that is bigger than 5GB
	url, err := r.Presign(15 * time.Minute)
	_, err := http.NewRequest("POST", url, strings.NewReader(""))
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

		assert.Equal(awsErr.Code(), "Bad Request")
		assert.Equal(awsErr.Message(), "Bad Request")
		assert.Equal(awsErr.Error(), 400)

		}
	}

	err := utils.DeleteStringObject(bucket, "AWIT.jpg")
	assert.Nil(err)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)
}

func TestPostObjectAuthenticatedRequestBadAccessKey(t *testing.T) 
{
	/*
	# Related python test

		https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1407
	
	# Goal

		The goal is to ensure that we can not post an object with wrong credentials

	# Test Process
		I create a bucket
		I try to post an object with a wrong access key.
		I Make an assertion to ensure that an error is returned.
		I make more assertions to ensure the returned error details match the exception.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	utils.CreateBucket(bucket)

	err, r := utils.Postwithwrongkey(bucket, "key1")
	url, err := r.Presign(15 * time.Minute)
	_, err := http.NewRequest("POST", url, strings.NewReader(""))
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidAccessKeyId")
			assert.Equal(awsErr.Message(), "InvalidAccessKeyId")
			assert.Equal(awsErr.Error(), 403)

		}

	}
	err := utils.DeleteStringObject(bucket, "key1")
	assert.Nil(err)
	err := utils.DeleteBucket(bucket)
	assert.Nil(err)

}


