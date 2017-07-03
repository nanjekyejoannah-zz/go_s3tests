package s3test

import (

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	 . "../Utilities"
)

func (suite *S3Suite) TestObjectListMany() {

	/* 
		Resource : object, method: list
		Scenario : list all keys 
		Assertion: pagination w/max_keys=2, no marker.
	*/

	assert := suite
	bucket := GetBucketName()
	maxkeys := int64(2)
	keys := []string{}
	objects := map[string]string{ "foo": "echo", "bar": "lima", "baz": "golf",}
	expected_keys := []string{"bar", "baz"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(svc, bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(len(resp.Contents), 2)
	assert.Equal(*resp.IsTruncated, true)
	assert.Equal(keys, expected_keys)

	resp, keys, errs := GetKeysWithMarker(svc, bucket, expected_keys[1])
	assert.Nil(errs)
	assert.Equal(len(resp.Contents), 1)
	assert.Equal(*resp.IsTruncated, false)
	expected_keys = []string{"foo"}

}

func (suite *S3Suite) TestObjectListMaxkeysNone() {

	/* 
		Resource : Object, Method: list
		Operation : List all keys
		Assertion : pagination w/o max_keys.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	ExpectedKeys :=[] string {"key1", "key2", "key3"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	resp, err := GetObjects(svc, bucket)
	assert.Nil(err)

	keys := []string{}
	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}
	assert.Equal(keys, ExpectedKeys)
	assert.Equal(*resp.MaxKeys, int64(1000))
	assert.Equal(*resp.IsTruncated, false)
}

func (suite *S3Suite) TestObjectListMaxkeysZero() {

	/* 
		Resource : object, method: get
		Operation : List all keys .
		Assertion: pagination w/max_keys=0.
	*/

	assert := suite
	bucket := GetBucketName()
	maxkeys := int64(0)
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	ExpectedKeys := []string(nil)


	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(svc, bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(ExpectedKeys, keys)
	assert.Equal(*resp.IsTruncated, false)
}

func (suite *S3Suite) TestObjectListMaxkeysOne() {

	/* 
		Resource : bucket, method: get
		Operation : List keys all keys. 
		Assertion: pagination w/max_keys=1, marker.
	*/

	assert := suite
	bucket := GetBucketName()
	maxkeys := int64(1)
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}
	EKeysMaxkey := []string{"key1"}
	EKeysMarker  := []string{"key2", "key3"}


	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	resp, keys, errr := GetKeysWithMaxKeys(svc, bucket, maxkeys)
	assert.Nil(errr)
	assert.Equal(EKeysMaxkey, keys)
	assert.Equal(*resp.IsTruncated, true)

	resp, keys, errs := GetKeysWithMarker(svc, bucket, EKeysMaxkey[0])
	assert.Nil(errs)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, EKeysMarker)
	
}

func (suite *S3Suite) TestObjectListPrefixDelimiterPrefixDelimiterNotExist() {

	/* 
		Resource : Object, method: ListObjects
		Scenario : list under prefix w/delimiter. 
		Assertion: finds nothing w/unmatched prefix and delimiter.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "y"
	delimeter := "z"
	var empty_list []*s3.Object
	objects := map[string]string{ "b/a/c": "echo", "b/a/g": "lima", "b/a/r": "golf", "g":"golf"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(svc, bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(keys, []string{})
	assert.Equal(prefixes, []string{})
	assert.Equal(empty_list, list.Contents)
}

func (suite *S3Suite) TestObjectListPrefixDelimiterDelimiterNotExist() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: over-ridden slash ceases to be a delimiter.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "b"
	delimeter := "z"
	objects := map[string]string{ "b/a/c": "echo", "b/a/g": "lima", "b/a/r": "golf",  "golffie": "golfyy",}
	expectedkeys := []string {"b/a/c", "b/a/g", "b/a/r" }

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(svc, bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(len(list.Contents), 3)
	assert.Equal(keys, expectedkeys)
	assert.Equal(prefixes, []string{})
}

func (suite *S3Suite) TestObjectListPrefixDelimiterPrefixNotExist() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: finds nothing w/unmatched prefix and delimiter.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "d"
	delimeter := "/"
	var empty_list []*s3.Object
	objects := map[string]string{ "b/a/r": "echo", "b/a/c": "lima", "b/a/g": "golf", "g": "g"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(svc, bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(keys, []string{})
	assert.Equal(prefixes, []string{})
	assert.Equal(empty_list, list.Contents)
}

func (suite *S3Suite) TestObjectListPrefixDelimiterAlt() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: non-slash delimiters.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "ba"
	delimeter := "a"
	objects := map[string]string{ "bar": "echo", "bazar": "lima", "cab": "golf", "foo": "g"}
	expected_keys := [] string {"bar"}
	expected_prefixes:= [] string {"baza"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(svc, bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)
	assert.Equal(*list.Delimiter, delimeter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
}

func (suite *S3Suite) TestObjectListPrefixDelimiterBasic() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix w/delimiter. 
		Assertion: returns only objects directly under prefix.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "foo/"
	delimeter := "/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf"}
	expected_keys := [] string {"foo/bar"}
	expected_prefixes := [] string {"foo/baz/"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimeterAndPrefix(svc, bucket, prefix, delimeter)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(*list.Delimiter, delimeter)
	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
}

func (suite *S3Suite) TestObjectListPrefixUnreadable() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: non-printable prefix can be specified.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "\x0a"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf"}
	expected_keys := [] string {}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(prefixes, expected_prefixes)
	assert.Equal(keys, expected_keys)

}

func (suite *S3Suite) TestObjectListPrefixNotExist() {

	/* 
		Resource : object, method: List
		Scenario : list under prefix. 
		Assertion: nonexistent prefix returns nothing.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "d"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

}

func (suite *S3Suite) TestObjectListPrefixNone() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: unspecified prefix returns everything.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := ""
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz", "quux" }
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
}

func (suite *S3Suite) TestObjectListPrefixEmpty() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: empty prefix returns everything.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := ""
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz", "quux" }
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)

}

func (suite *S3Suite) TestObjectListPrefixAlt() {

	/* 
		Resource : object, method: list
		Scenario : list under prefix. 
		Assertion: prefixes w/o delimiters.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "ba"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "foo": "golf",}
	expected_keys := [] string {"bar", "baz"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListPrefixBasic() {

	/* 
		Resource : bucket, method: get
		Scenario : list under prefix. 
		Assertion: returns only objects under prefix.
	*/

	assert := suite
	bucket := GetBucketName()
	prefix := "foo/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz": "lima", "quux": "golf",}
	expected_keys := [] string {"foo/bar", "foo/baz"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithPrefix(svc, bucket, prefix)
	assert.Nil(errr)
	assert.Equal(*list.Prefix, prefix)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterNotExist() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: unused delimiter is not found.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "/"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterNone() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: unspecified delimiter defaults to none.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := " "
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterEmpty () {

	 
		// Resource : object, method: list
		// Scenario : list . 
		// Assertion: empty delimiter can be specified.
	

	assert := suite
	bucket := GetBucketName()
	delimiter := " "
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterUnreadable() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: non-printable delimiter can be specified.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "\x0a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"bar", "baz", "cab", "foo"}
	expected_prefixes := [] string {}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterDot() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: dot delimiter characters.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "."
	objects := map[string]string{ "b.ar": "echo", "b.az": "lima", "c.ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b.", "c."}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterPercentage() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: percentage delimiter characters.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "%"
	objects := map[string]string{ "b%ar": "echo", "b%az": "lima", "c%ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b%", "c%"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterWhiteSpace() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: whitespace delimiter characters.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := " "
	objects := map[string]string{ "b ar": "echo", "b az": "lima", "c ab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"b ", "c "}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterAlt() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: non-slash delimiter characters.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "cab": "golf", "foo": "golf",}
	expected_keys := [] string {"foo"}
	expected_prefixes := [] string {"ba", "ca"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)
	
}

func (suite *S3Suite) TestObjectListDelimiterBasic() {

	/* 
		Resource : object, method: list
		Scenario : list . 
		Assertion: prefixes in multi-component object names.
	*/

	assert := suite
	bucket := GetBucketName()
	delimiter := "/"
	objects := map[string]string{ "foo/bar": "echo", "foo/baz/xyzzy": "lima", "quux/thud": "golf", "asdf": "golf",}
	expected_keys := [] string {"asdf"}
	expected_prefixes := [] string {"foo/", "quux/"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	list, keys, prefixes, errr := ListObjectsWithDelimiter(svc, bucket, delimiter)
	assert.Nil(errr)
	assert.Equal(*list.Delimiter, delimiter)

	assert.Equal(keys, expected_keys)
	assert.Equal(len(prefixes), 2)
	assert.Equal(prefixes, expected_prefixes)
	
}

//............................................Test Get object with marker...................................

func (suite *S3Suite) TestObjectListMarkerBeforeList() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker before list.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := "aaa"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := [] string {"bar", "baz", "quux"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(keys, expected_keys)
	assert.Equal(*resp.IsTruncated, false)

	err = DeleteObjects(svc, bucket)
	err = DeleteBucket(svc, bucket)
	assert.Nil(err)
	
}

func (suite *S3Suite) TestObjectListMarkerAfterList() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker after list.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := "zzz"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string(nil)

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)
	
}

func (suite *S3Suite) TestObjectListMarkerNotInList() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: marker not in list.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := "blah"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"quux"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(keys, expected_keys)
}

func (suite *S3Suite) TestObjectListMarkerUnreadable() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: non-printing marker.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := "\x0a"
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"bar", "baz", "quux"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)
	
}

func (suite *S3Suite) TestObjectListMarkerEmpty() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: no pagination, empty marker.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := ""
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}
	expected_keys := []string{"bar", "baz", "quux"}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, keys, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	assert.Equal(*resp.IsTruncated, false)
	assert.Equal(keys, expected_keys)
	
}

func (suite *S3Suite) TestObjectListMarkerNone() {

	/* 
		Resource : object, method: get
		Scenario : list all objects. 
		Assertion: no pagination, no marker.
	*/

	assert := suite
	bucket := GetBucketName()
	marker := ""
	objects := map[string]string{ "bar": "echo", "baz": "lima", "quux": "golf",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)
	

	resp, _, errr := GetKeysWithMarker(svc, bucket, marker)
	assert.Nil(errr)
	assert.Equal(*resp.Marker, marker)
	
}


func (suite *S3Suite) TestObjectReadNotExist() {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/

	assert := suite
	bucket := GetBucketName()

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	_, err = GetObject(svc, bucket, "key6")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchKey")
			assert.Equal(awsErr.Message(), "")

		}
	}

}

func (suite *S3Suite) TestObjectReadFromNonExistantBucket() {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/
	assert := suite
	non_exixtant_bucket := "bucketz"

	_, err := GetObject(svc, non_exixtant_bucket, "key6")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}

	}

}

func (suite *S3Suite) TestObjectWriteToNonExistantBucket() {

	/*
		Reource object : method: get 
		Operation : read object
		Assertion : read contents that were never written
	*/

	assert := suite
	non_exixtant_bucket := "bucketz"
	objects := map[string]string{ "key1": "echo", "key2": "lima", "key3": "golf",}

	err := CreateObjects(svc, non_exixtant_bucket, objects)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}

	}

}

func (suite *S3Suite) TestObjectWriteReadUpdateReadDelete() {

	assert := suite
	bucket := GetBucketName()
	key := "key1"

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	// Write object
	PutObjectToBucket(svc, bucket, key, "hello")
	assert.Nil(err)

	// Read object
	result, _ := GetObject(svc, bucket, key)
	assert.Equal(result, "hello")

	//Update object
	PutObjectToBucket(svc, bucket, key, "Come on !!")
	assert.Nil(err)

	// Read object again
	result, _ = GetObject(svc, bucket, key)
	assert.Equal(result, "Come on !!")

	err = DeleteObjects(svc, bucket)
	assert.Nil(err)

	// If object was well deleted, there shouldn't be an error at this point
	err = DeleteBucket(svc, bucket)
	assert.Nil(err)
}

func (suite *S3Suite) TestObjectDeleteAll() {

	// Reading content that was never written should fail
	assert := suite
	bucket := GetBucketName()
	var empty_list []*s3.Object
	key := "key5"
	key1 := "key6"

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	PutObjectToBucket(svc, bucket, key, "hello")
	PutObjectToBucket(svc, bucket, key1, "foo")
	assert.Nil(err)
	objects, err := ListObjects(svc, bucket)
	assert.Nil(err)
	assert.Equal(2, len(objects))

	err = DeleteObjects(svc, bucket)
	assert.Nil(err)

	objects, err = ListObjects(svc, bucket)
	assert.Nil(err)
	assert.Equal(empty_list, objects)

}

func (suite *S3Suite) TestObjectCopyBucketNotFound() {

	// copy from non-existent bucket

	assert := suite
	bucket := GetBucketName()
	item := "key1"
	other := GetBucketName()

	source := bucket + "/" + item

	err := CreateBucket(svc, bucket)
	assert.Nil(err)

	// Write object
	PutObjectToBucket(svc, bucket, item, "hello")
	assert.Nil(err)

	err = CopyObject(svc, other, source, item)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}

	}

}

func (suite *S3Suite) TestObjectCopyKeyNotFound() {

	assert := suite
	bucket := GetBucketName()
	item := "key1"
	other := GetBucketName()

	source := bucket + "/" + item

	err := CreateBucket(svc, bucket)
	err = CreateBucket(svc, other)
	assert.Nil(err)

	err = CopyObject(svc, other, source, item)
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchKey")
			assert.Equal(awsErr.Message(), "")
		}

	}

}

//.....................................Test Getting Ranged Objects....................................................................................................................

func (suite *S3Suite) TestRangedRequest() {

	//getting objects in a range should return correct data

	assert := suite
	bucket := GetBucketName()
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(svc, bucket)
	PutObjectToBucket(svc, bucket, key, content)

	resp, data, err = GetObjectWithRange(svc, bucket, key, "bytes=4-7")
	assert.Nil(err)
	assert.Equal(data, content[4:8])
	assert.Equal(*resp.AcceptRanges, "bytes")
}

func (suite *S3Suite) TestRangedRequestSkipLeadingBytes() {

	//getting objects in a range should return correct data

	assert := suite
	bucket := GetBucketName()
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(svc, bucket)
	PutObjectToBucket(svc, bucket, key, content)

	resp, data, err = GetObjectWithRange(svc, bucket, key, "bytes=4-")
	assert.Nil(err)
	assert.Equal(data, content[4:])
	assert.Equal(*resp.AcceptRanges, "bytes")

}

func (suite *S3Suite) TestRangedRequestReturnTrailingBytes() {

	//getting objects in a range should return correct data

	assert := suite
	bucket := GetBucketName()
	key := "key"
	content := "testcontent"

	var data string
	var resp *s3.GetObjectOutput


	err := CreateBucket(svc, bucket)
	PutObjectToBucket(svc, bucket, key, content)

	resp, data, err = GetObjectWithRange(svc, bucket, key, "bytes=-8")
	assert.Nil(err)
	assert.Equal(data, content[3:11])
	assert.Equal(*resp.AcceptRanges, "bytes")
}

func (suite *S3Suite) TestRangedRequestInvalidRange() {

	//getting objects in unaccepted range returns invalid range

	assert := suite
	bucket := GetBucketName()
	key := "key"
	content := "testcontent"

	err := CreateBucket(svc, bucket)
	PutObjectToBucket(svc, bucket, key, content)

	_, _, err = GetObjectWithRange(svc, bucket, key, "bytes=40-50")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidRange")
			assert.Equal(awsErr.Message(), "")

		}
	}
}

func (suite *S3Suite) TestRangedRequestEmptyObject() {

	//getting a range of an empty object returns invalid range

	assert := suite
	bucket := GetBucketName()
	key := "key"
	content := ""

	err := CreateBucket(svc, bucket)
	PutObjectToBucket(svc, bucket, key, content)

	_, _, err = GetObjectWithRange(svc, bucket, key, "bytes=40-50")
	assert.NotNil(err)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "InvalidRange")
			assert.Equal(awsErr.Message(), "")

		}
	}
}

func (suite *S3Suite) TestObjectHeadZeroBytes() {

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "bar": "",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	assert.Nil(err)

	resp, err := GetObject(svc, bucket, "bar")
	assert.Nil(err)
	assert.Equal(0, len(resp))
}

func (suite *S3Suite) TestObjectSetGetMetadataNoneToGood() {

	assert := suite
	metadata := map[string]*string{ "mymeta": nil,}
	got := GetSetMetadata(metadata)
	assert.Equal(got, metadata)
}

func (suite *S3Suite) TestObjectSetGetMetadataNoneToEmpty() {

	assert := suite
	metadata := map[string]*string{ "": nil,}
	got := GetSetMetadata(metadata)
	assert.Equal(got, metadata)
}

func (suite *S3Suite) TestObjectSetGetMetadataOverwriteToGood() {

	assert := suite

	oldmetadata := map[string]*string{ "meta1": nil,}
	got := GetSetMetadata(oldmetadata)
	assert.Equal(got, oldmetadata)

	newmetadata := map[string]*string{ "meta2": nil,}
	got = GetSetMetadata(newmetadata)
	assert.Equal(got, newmetadata)
}

func (suite *S3Suite) TestObjectSetGetMetadataOverwriteToEmpty() {

	assert := suite

	oldmetadata := map[string]*string{ "meta1": nil,}
	got := GetSetMetadata(oldmetadata)
	assert.Equal(got, oldmetadata)

	newmetadata := map[string]*string{ "": nil,}
	got = GetSetMetadata(newmetadata)
	assert.Equal(got, newmetadata)
}

//..............................................SSE-C encrypted transfer....................................................

func (suite *S3Suite) TestEncryptedTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestEncryptedTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1KB
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestEncryptedTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 1MB
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestEncryptedTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := EncryptionSSECustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

//.................................SSE and KMS......................................................................

func (suite *S3Suite) TestSSEKMSbarbTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSbarbTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSbarbTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSbarbTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-C encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSTransfer13B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 13 bytes
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSkeyIdCustomerWrite(svc, 13)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSTransfer1MB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 mega byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1024*1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSTransfer1KB() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 kilobyte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1024)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSTransfer1B() {

	/* 
		Resource : object, method: put
		Scenario : Test SSE-KMS encrypted transfer 1 byte
		Assertion: success.
	*/
	assert := suite

	rdata, data, err := SSEKMSCustomerWrite(svc, 1)
	assert.Nil(err)
	assert.Equal(rdata, data)
}

func (suite *S3Suite) TestSSEKMSPresent() {

	/* 
		Resource : object, method: put
		Scenario : write encrypted with SSE-KMS and read without SSE-KMS
		Assertion: success.
	*/
	assert := suite
	bucket := GetBucketName()

	err := CreateBucket(svc, bucket)

	err = WriteSSEKMSkeyId(svc, bucket, "kay1", "test", []string{"AES256"}, "barbican_key_id")
	data, err := GetObject(svc, bucket, "kay1")
	assert.Nil(err)
	assert.Equal("test", data)
}

//...................................... get object with conditions....................

func (suite *S3Suite) TestGetObjectIfmatchGood(){

	/* 
		Resource : object, method: get
		Scenario : get w/ If-Match: the latest ETag 
		Assertion: suceeds.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "foo": "bar",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	object, err := GetObj(svc, bucket, "foo")

	got, err := GetObjectWithIfMatch(svc, bucket, "foo", *object.ETag)
	assert.Nil(err)
	assert.Equal(got, "bar")

}

func (suite *S3Suite) TestGetObjectIfmatchFailed(){

	/* 
		Resource : object, method: get
		Scenario : get w/ If-Match: bogus ETag 
		Assertion: fails.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "foo": "bar",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)

	_, err = GetObjectWithIfMatch(svc, bucket, "foo", "ABCORZ")
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "PreconditionFailed")
			assert.Equal(awsErr.Message(), "")
		}
	}

}

func (suite *S3Suite) TestGetObjectIfNoneMatchGood(){

	/* 
		Resource : object, method: get
		Scenario : get w/ If-None-Match: the latest ETag
		Assertion: fails.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "foo": "bar",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	object, err := GetObj(svc, bucket, "foo")

	_, err = GetObjectWithIfNoneMatch(svc, bucket, "foo", *object.ETag)
	assert.NotNil(err)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NotModified")
			assert.Equal(awsErr.Message(), "Not Modified")
		}
	}

}

func (suite *S3Suite) TestGetObjectIfNoneMatchFailed(){

	/* 
		Resource : object, method: get
		Scenario : get w/ If-None-Match: bogus ETag 
		Assertion: suceeds.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "foo": "bar",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)

	got, err := GetObjectWithIfNoneMatch(svc, bucket, "foo", "ABCORZ")
	assert.Nil(err)
	assert.Equal(got, "bar")
}

func (suite *S3Suite) TestGetObjectIfModifiedSinceGood(){

	/* 
		Resource : object, method: get
		Scenario : get w/ If-Modified-Since: before
		Assertion: suceeds.
	*/

	assert := suite
	bucket := GetBucketName()
	objects := map[string]string{ "foo": "bar",}

	err := CreateBucket(svc, bucket)
	err = CreateObjects(svc, bucket, objects)
	object, err := GetObj(svc, bucket, "foo")

	got, err := GetObjectWithIfModifiedSince(svc, bucket, "foo", time.Time("Sat, 29 Oct 1994 19:43:31 GMT"))
	assert.Nil(err)
	assert.Equal(got, "bar")
}

