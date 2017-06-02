import (
	"testing"
	assert "github.com/stretchr/testify/require"

	"helpers/utils"
)

func TestObjectCRUD(t *testing.T) {
	/*
	
	https://github.com/ceph/s3-tests/blob/master/s3tests/functional/test_s3.py#L1110

	Goal is to test if we can write , read , update and delete an object.

	## Testing object read and write

	I just created a new bucket , wrote some data there and wrote an assertion 
	to see if the data I wrote exists and can be read.

	## Testing update

	There is no update method in the Go SDK. So I decided to write new data in the same 
	bucket and using an existing key. This should overwrite the current data.
	I therefore write an assertion checking if the new data has been writen.

	## testing object Delete
	I simply call a delete method from the SDK with a bucket name and key. And perform an 
	assertion to ensure there is no data after delete.
	*/
	assert := assert.New(t)
	bucketname := "bucket1"
	key : "key1"
	word1 := "cheers"
	word2 :+ "cheers again"


	bucket = utils.CreateBucket(bucketname)
	// write and read
	utils.WriteObject(bucket, key, word1)
	read = utils.ReadObject(bucket, key)
	assert.Equal(read, word1) 

	// update
	utils.WriteObject(bucket, key)
	read = utils.ReadObject(bucket, key)
	assert.Equal(read, word2) 

	//delete
	utils.DeleteObject(bucket, key)
	assert.Equal(read, "")

	utils.DeleteBucket

}

func TestMultipleObjectDelete(t *testing.T) {
	assert := assert.New(t)
	content = "Hello world"

	bucket0, key0 := "bucket0", "key0"
	bucket1, key1 := "bucket1", "key1"
	bucket2, key2 := "bucket2", "key2"



}