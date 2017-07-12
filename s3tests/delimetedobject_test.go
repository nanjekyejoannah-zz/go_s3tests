
package s3test

import (

	"github.com/stretchr/testify/suite"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	

	. "../Utilities"
)

type HeadSuite struct {
	suite.Suite
}

func (suite *HeadSuite) TestObjectListPrefixDelimiterPrefixDelimiterNotExist() {

	/* 
		Resource : Object, method: ListObjects
		Scenario : list under prefix w/delimiter. 
		Assertion: finds nothing w/unmatched prefix and delimiter.
	*/

	svc := GetConn()

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
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {

			assert.Equal(awsErr.Code(), "NoSuchBucket")
			assert.Equal(awsErr.Message(), "")
		}
	}
	assert.Equal(keys, []string{})
	assert.Equal(prefixes, []string{})
	assert.Equal(empty_list, list.Contents)
}

func (suite *HeadSuite) TestObjectListPrefixDelimiterDelimiterNotExist() {

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

func (suite *HeadSuite) TestObjectListPrefixDelimiterPrefixNotExist() {

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

func (suite *HeadSuite) TestObjectListPrefixDelimiterAlt() {

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

func (suite *HeadSuite) TestObjectListPrefixDelimiterBasic() {

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

func (suite *HeadSuite) TestObjectListPrefixUnreadable() {

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

func (suite *HeadSuite) TestObjectListPrefixNotExist() {

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

func (suite *HeadSuite) TestObjectListPrefixNone() {

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

func (suite *HeadSuite) TestObjectListPrefixEmpty() {

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

func (suite *HeadSuite) TestObjectListPrefixAlt() {

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

func (suite *HeadSuite) TestObjectListPrefixBasic() {

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

func (suite *HeadSuite) TestObjectListDelimiterNotExist() {

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

func (suite *HeadSuite) TestObjectListDelimiterNone() {

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

func (suite *HeadSuite) TestObjectListDelimiterEmpty () {

	 
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

func (suite *HeadSuite) TestObjectListDelimiterUnreadable() {

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

func (suite *HeadSuite) TestObjectListDelimiterDot() {

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

func (suite *HeadSuite) TestObjectListDelimiterPercentage() {

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

func (suite *HeadSuite) TestObjectListDelimiterWhiteSpace() {

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

func (suite *HeadSuite) TestObjectListDelimiterAlt() {

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

func (suite *HeadSuite) TestObjectListDelimiterBasic() {

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

func (suite *HeadSuite) TearDownTest() {

	DeletePrefixedBuckets(svc)
}

