package s3_test

import (
	assert "github.com/stretchr/testify/require"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	."github.com/nanjekyejoannah/go_s3tests"
)

//.............Bucket Operations.............................................................

func TestBucketCreateReadDelete(t *testing.T) {

	/* 
		Resource : bucket, method: create/delete
		Scenario : create and delete bucket. 
		Assertion: bucket exists after create and is gone after delete.
	*/

	assert := assert.New(t)
	bucket := "bucket4"

	err := CreateBucket(bucket)
	assert.Nil(err)

	bkts, err := ListBuckets()
	assert.Nil(err)
	assert.Equal(true, contains(bkts, bucket))

	
	err = DeleteBucket(bucket)

	//make sure it is gone
	err = DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}
}

func TestBucketCreateExistingBucket(t *testing.T) {

	/* 
		Resource : bucket, method: create/delete
		Scenario : create and delete bucket. 
		Assertion: bucket exists after create and is gone after delete.
	*/

	assert := assert.New(t)
	bucket := "bucket1"

	err := CreateBucket(bucket)
	assert.Nil(err)

	//try to create it again
	err = CreateBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "BucketAlreadyExists")
			assert.Equal(awsErr.Message(), "")
		}
	}

	err = DeleteBucket(bucket)
	assert.NotNil(err)

}

func TestBucketDeleteNotExist(t *testing.T) {

	/* 
		Resource : bucket, method: delete
		Scenario : non existant bucket 
		Assertion: fails NoSuchBucket.
	*/

	assert := assert.New(t)
	bucket := "bucketZZ"

	err := DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}

}

func TestBucketDeleteNotEmpty(t *testing.T) {

	/* 
		Resource : bucket, method: delete
		Scenario : bucket not empty 
		Assertion: fails BucketNotEmpty.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	objects := map[string]string{ "key1": "echo",}

	err := CreateBucket(bucket)
	assert.Nil(err)

	err = CreateObjects(bucket, objects)

	err = DeleteBucket(bucket)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "BucketNotEmpty")
			assert.Equal(awsErr.Message(), "")
		}
	}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListEmpty(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : bucket not empty 
		Assertion: empty buckets return no contents.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	var empty_list []*s3.Object

	err := CreateBucket(bucket)
	assert.Nil(err)

	resp, err := GetObjects(bucket) 
	assert.Nil(err)
	assert.Equal(empty_list, resp.Contents)

	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestBucketListDistinct(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : bucket not empty 
		Assertion: distinct buckets have different contents.
	*/

	assert := assert.New(t)
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	objects1 := map[string]string{ "key1": "Hello",}
	objects2 := map[string]string{ "key2": "Manze",}

	err := CreateBucket(bucket1)
	err = CreateBucket(bucket2)
	assert.Nil(err)

	err = CreateObjects(bucket1, objects1)
	err = CreateObjects(bucket2, objects2)

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

func TestObjectListMany(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list all keys 
		Assertion: pagination w/max_keys=2, no marker.
	*/

	assert := assert.New(t)
	bucket := "bucket10"
	maxkeys := int64(2)
	keys := []string{}
	objects := map[string]string{ "foo": "echo", "bar": "lima", "baz": "golf",}
	expected_keys := []string{"bar", "baz"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(len(resp.Contents), 2)
	assert.Equal(*resp.IsTruncated, true)
	assert.Equal(keys, expected_keys)

	resp, keys, errs := GetKeysWithMarker(bucket, expected_keys[1])
	assert.Nil(errs)
	assert.Equal(len(resp.Contents), 1)
	assert.Equal(*resp.IsTruncated, false)
	expected_keys = []string{"foo"}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListMaxkeysInvalid(t *testing.T) {

	/* 
		Resource : Bucket , Method : get
		Scenario : List all keys with invalid max key should fail. 
		Assertion : invalid max_keys
		Apparently it is passing on RGW. It should be failing with a max key value less than Zero.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	maxkeys := int64(-9)
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.NotNil(err)

	_, _, err = GetKeysWithMaxKeys(bucket, maxkeys)
	assert.Nil(err)


	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidArgument")
			assert.Equal(awsErr.Message(), "")

		}
	}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestBucketListMaxkeysNone(t *testing.T) {

	/* 
		Resource : Bucket, Method: get
		Operation : List all keys
		Assertion : pagination w/o max_keys.
	*/

	assert := assert.New(t)
	bucket := "bucket4"
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	ExpectedKeys :=[] string {"key1", "key2", "key3"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)

	resp, err := GetObjects(bucket)
	assert.Nil(err)

	keys := []string{}
	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}
	assert.Equal(keys, ExpectedKeys)
	assert.Equal(*resp.MaxKeys, int64(1000))
	assert.Equal(*resp.IsTruncated, false)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestBucketListMaxkeysZero(t *testing.T) {

	/* 
		Resource : bucket, method: get
		Operation : List all keys .
		Assertion: pagination w/max_keys=0.
	*/

	assert := assert.New(t)
	bucket := "bucketz"
	maxkeys := int64(0)
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	ExpectedKeys := []string(nil)


	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(ExpectedKeys, keys)
	assert.Equal(*resp.IsTruncated, false)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestBucketListMaxkeysOne(t *testing.T) {

	/* 
		Resource : bucket, method: get
		Operation : List keys all keys. 
		Assertion: pagination w/max_keys=1, marker.
	*/

	assert := assert.New(t)
	bucket := "buckety"
	maxkeys := int64(1)
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	EKeysMaxkey := []string{"key1"}
	EKeysMarker  := []string{"key2", "key3"}


	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(EKeysMaxkey, keys)
	assert.Equal(*resp.IsTruncated, true)

	resp, keys, errs := GetKeysWithMarker(bucket, EKeysMaxkey[0])
	assert.Nil(errs)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, EKeysMarker)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListPrefixDelimiterPrefixDelimiterNotExist(t *testing.T) {

	/* 
		Resource : Object, method: ListObjects
		Scenario : list under prefix w/delimiter. 
		Assertion: finds nothing w/unmatched prefix and delimiter.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "y"
	delimeter := "z"
	var empty_list []*s3.Object
	objects := map[string]string{ "b/a/c": "echo", "b/a/g": "lima", "b/a/r": "golf", "g":"golf"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(keys, []string{})
	assert.Equal(prefixes, []string{})
	assert.Equal(empty_list, list.Contents)


	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixDelimiterDelimiterNotExist(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: over-ridden slash ceases to be a delimiter.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "b"
	delimeter := "z"
	objects := map[string]string{ "b/a/c": "echo", "b/a/g": "lima", "b/a/r": "golf",  "golffie": "golfyy",}
	expectedkeys := []string {"b/a/c", "b/a/g", "b/a/r" }

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(len(list.Contents), 3)
	assert.Equal(keys, expectedkeys)
	assert.Equal(prefixes, []string{})

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	

}

func TestObjectListPrefixDelimiterPrefixNotExist(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: finds nothing w/unmatched prefix and delimiter.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "d"
	delimeter := "/"
	var empty_list []*s3.Object
	objects := map[string]string{ "b/a/r": "echo", "b/a/c": "lima", "b/a/g": "golf", "g": "g"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(keys, []string{})
	assert.Equal(prefixes, []string{})
	assert.Equal(empty_list, list.Contents)


	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixDelimiterAlt(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: non-slash delimiters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "ba"
	delimeter := "a"
	objects := map[string]string{ "bar": "echo", "bazar": "lima", "cab": "golf", "foo": "g"}
	expected_keys := [] string {"bar"}
	expected_prefixes:= [] string {"baza"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)
	assert.Equal(*list.Delimiter, delimeter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixDelimiterBasic(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: returns only objects directly under prefix.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "foo/"
	delimeter := "/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf"}
	expected_keys := [] string {"foo/bar"}
	expected_prefixes := [] string {"foo/baz/"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(*list.Delimiter, delimeter)
	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixUnreadable(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: non-printable prefix can be specified.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "\x0a"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf"}
	expected_keys := [] string {}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(prefixes, expected_prefixes)
	assert.Equal(keys, expected_keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixNotExist(t *testing.T) {

	/* 
		Resource : object, method: List
		Scenario : list under prefix. 
		Assertion: nonexistent prefix returns nothing.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "d"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixNone(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: unspecified prefix returns everything.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := ""
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz", "quux" }
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
}

func TestObjectListPrefixEmpty(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: empty prefix returns everything.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := ""
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz", "quux" }
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)

}

func TestObjectListPrefixAlt(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: prefixes w/o delimiters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "ba"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "foo": "golf",}
	expected_keys := [] string {"bar", "baz"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListPrefixBasic(t *testing.T) {

	/* 
		Resource : bucket, method: get
		Scenario : list under prefix. 
		Assertion: returns only objects under prefix.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	prefix := "foo/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterNotExist(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: unused delimiter is not found.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "/"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterNone(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: unspecified delimiter defaults to none.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := " "
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterEmpty(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: empty delimiter can be specified.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := " "
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterUnreadable(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: non-printable delimiter can be specified.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "\x0a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterDot(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: dot delimiter characters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "."
	objects := map[string]string{ "b.ar": "echo", "b.az": "lima", "c.ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b.", "c."}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterWhiteSpace(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: whitespace delimiter characters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := " "
	objects := map[string]string{ "b ar": "echo", "b az": "lima", "c ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b ", "c "}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterPercentage(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: percentage delimiter characters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "%"
	objects := map[string]string{ "b%ar": "echo", "b%az": "lima", "c%ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b%", "c%"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterAlt(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: non-slash delimiter characters.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"ba", "ca"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListDelimiterBasic(t *testing.T) {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: prefixes in multi-component object names.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	delimiter := "/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf", "asdf": "golf",}
	expected_keys := [] string {"asdf"}
	expected_prefixes := [] string {"foo/", "quux/"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

//............................................Test Get object with marker...................................

func TestBucketListMarkerBeforeList(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker before list.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := "aaa"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := [] string {"bar", "baz", "quux"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(keys, expected_keys)
	assert.Equal(*resp.IsTruncated, false)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestBucketListMarkerAfterList(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker after list.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := "zzz"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string(nil)

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListMarkerNotInList(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker not in list.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := "blah"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"quux"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(keys, expected_keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListMarkerUnreadable(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: non-printing marker.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := "\x0a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"bar", "baz", "quux"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListMarkerEmpty(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: no pagination, empty marker.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := ""
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"bar", "baz", "quux"}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}

func TestObjectListMarkerNone(t *testing.T) {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: no pagination, no marker.
	*/

	assert := assert.New(t)
	bucket := "bucket1"
	marker := ""
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}

	err := CreateBucket(bucket)
	err = CreateObjects(bucket, objects)
	assert.Nil(err)
	

	resp, _, errr := GetKeysWithMarker(bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	
	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
	assert.Nil(err)
	
}


func TestObjectReadNotExist(t *testing.T) {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/

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

		}
	}

	err = DeleteBucket(bucket1)

}

func TestObjectReadFromNonExistantBucket(t *testing.T) {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/

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

func TestObjectWriteToNonExistantBucket(t *testing.T) {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/

	assert := assert.New(t)
	non_exixtant_bucket := "bucketz"
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}

	err := CreateObjects(non_exixtant_bucket, objects)
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
	bucket := "bucket1"
	key := "key5"
	key1 := "key6"

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

//.....................................Test Getting Ranged Objects....................................................................................................................

func TestRangedRequest(t *testing.T) {

	//getting objects in a range should return correct data

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(bucket)
	err = PutObjectToBucket(bucket, key, content)

	resp, data, err = GetObjectWithRange(bucket, key, "bytes=4-7")
	assert.Nil(err)
	assert.Equal(data, content[4:8])
	assert.Equal(*resp.AcceptRanges, "bytes")

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
}

func TestRangedRequestSkipLeadingBytes(t *testing.T) {

	//getting objects in a range should return correct data

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(bucket)
	err = PutObjectToBucket(bucket, key, content)

	resp, data, err = GetObjectWithRange(bucket, key, "bytes=4-")
	assert.Nil(err)
	assert.Equal(data, content[4:])
	assert.Equal(*resp.AcceptRanges, "bytes")

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
}

func TestRangedRequestReturnTrailingBytes(t *testing.T) {

	//getting objects in a range should return correct data

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(bucket)
	err = PutObjectToBucket(bucket, key, content)

	resp, data, err = GetObjectWithRange(bucket, key, "bytes=-8")
	assert.Nil(err)
	assert.Equal(data, content[3:11])
	assert.Equal(*resp.AcceptRanges, "bytes")

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
}

func TestRangedRequestInvalidRange(t *testing.T) {

	//getting objects in unaccepted range returns invalid range

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key"
	content := "testcontent"

	err := CreateBucket(bucket)
	err = PutObjectToBucket(bucket, key, content)

	_, _, err = GetObjectWithRange(bucket, key, "bytes=40-50")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidRange")
			assert.Equal(awsErr.Message(), "")

		}
	}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
}

func TestRangedRequestEmptyObject(t *testing.T) {

	//getting a range of an empty object returns invalid range

	assert := assert.New(t)
	bucket := "bucket1"
	key := "key"
	content := ""

	err := CreateBucket(bucket)
	err = PutObjectToBucket(bucket, key, content)

	_, _, err = GetObjectWithRange(bucket, key, "bytes=40-50")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidRange")
			assert.Equal(awsErr.Message(), "")

		}
	}

	err = DeleteObjects(bucket)
	err = DeleteBucket(bucket)
}