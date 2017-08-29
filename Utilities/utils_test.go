package helpers

import ( 

  "testing"
  "github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {

	assert := assert.New(t)

	res0 := string(10)
	res1 := String(10)

	assert.NotEqual(res0, res1)

}

func TestGetBucketName(t *testing.T) {

	assert := assert.New(t)

	res0 := GetBucketName()
	res1 := GetBucketName()

	assert.NotEqual(res0, res1)

}

func TestContains(t *testing.T) {

	assert := assert.New(t)

	args := []string{"a", "b"}

	assert.Equal(true, Contains(args, "a"))

}
