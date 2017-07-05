package s3test

import (
	
	"github.com/stretchr/testify/suite"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/spf13/viper"
)

type HostStyleSuite struct {
    suite.Suite
}

type s3BucketTest struct { 
	bucket  string
	url     string
	errCode string
}

var domain = fmt.Sprintf( "http://%s", viper.GetString("s3main.endpoint") )

func getUrl(bktname string)(string){
	url := fmt.Sprintf("%s%s", domain, bktname)

	return url
}

func (suite *HostStyleSuite) TestAccelerateNoSSLBucketBuild() {

	assert := suite

	tests := []s3BucketTest{
		{"a.b.c", getUrl("a.b.c"), ""},
		{"a..bc", getUrl("a..bc"), ""},
	}

	for _, test := range tests {
		req, _ := svc.ListObjectsRequest(&s3.ListObjectsInput{Bucket: &test.bucket})
		req.Build()
		assert.Equal (test.url, req.HTTPRequest.URL.String())
	}
}

func (suite *HostStyleSuite) TestHostStyleBucketBuildNoSSL() {

	//svc := s3.New(unit.Session, &aws.Config{DisableSSL: aws.Bool(true)})

	assert := suite

	tests := []s3BucketTest{
		{"abc", getUrl("abc"), ""},
		{"a.b.c", getUrl("a.b.c"), ""},
		{"a$b$c", getUrl("a%24b%24c"), "InvalidParameterException"},
	}

	for _, test := range tests {
		req, _ := svc.ListObjectsRequest(&s3.ListObjectsInput{Bucket: &test.bucket})
		req.Build()
		assert.Equal (test.url, req.HTTPRequest.URL.String())
	}
}

