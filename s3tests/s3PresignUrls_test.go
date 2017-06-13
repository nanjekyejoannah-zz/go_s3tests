package urls

import (
	assert "github.com/stretchr/testify/require"
	"testing"
)

func TestGeneratePresignedUrlGetObject(t *testing.T) {

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key1"
	url := ""

	err := CreateBucket(bucket)
	assert.Nil(err)

	err = PutObjectToBucket(bucket, key, "hello")
	assert.Nil(err)

	url, err = GeneratePresignedUrlGetObject(bucket, key)
	assert.Nil(err)
	assert.NotEqual("", url)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}
