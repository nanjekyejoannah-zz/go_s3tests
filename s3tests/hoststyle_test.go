package s3test

import (
	"github.com/stretchr/testify/suite"

	//"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/spf13/viper"
	"net/http"
	//"net/url"

	"time"

	. "../Utilities"
)

type HostStyleSuite struct {
	suite.Suite
}

type s3TestCase struct {
	bucket string
	key    string
	url    string
}

var bucket = "bucket1"
var key = "key1"

func (suite *HostStyleSuite) SetupSuite() {

	bucket := GetBucketName()
	objects := map[string]string{key: "echo"}

	_ = CreateBucket(svc, bucket)
	_ = CreateObjects(svc, bucket, objects)
}

func (suite *HostStyleSuite) TestOrdinaryCallingFormatNoSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + endpoint + "/" + bucket + "/" + key

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *HostStyleSuite) TestOrdinaryCallingFormatSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + endpoint + "/" + bucket + "/" + key

	//u, _ := url.Parse(ur)

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

// func (suite *HostStyleSuite) TestProtocolIndependentOrdinaryCallingFormatSSL() {

// 	assert := suite
// 	signer := v4.NewSigner(Creds)
// 	endpoint := viper.GetString("s3main.endpoint")
// 	expectBody := []byte("abc123")

// 	ur := endpoint + "/" + bucket + "/" + key

// 	req, err := http.NewRequest("GET", ur, nil)

// 	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
// 	assert.Nil(err)

// 	resp, err := http.DefaultClient.Do(req)
// 	assert.Nil(err)
// 	assert.Equal(http.StatusOK, resp.StatusCode)
// }

func (suite *HostStyleSuite) TestSubdomainCallingFormatNoSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + bucket + "." + endpoint + "/" + "/" + key

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *HostStyleSuite) TestSubdomainCallingFormatSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + bucket + "." + endpoint + "/" + "/" + key

	//u, _ := url.Parse(ur)

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *HostStyleSuite) TestVHostCallingFormatNoSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	//endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + bucket + "/" + key

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *HostStyleSuite) TestVHostCallingFormatSSL() {

	assert := suite
	signer := v4.NewSigner(Creds)
	//endpoint := viper.GetString("s3main.endpoint")
	expectBody := []byte("abc123")

	ur := "http://" + bucket  + "/" + key

	req, err := http.NewRequest("GET", ur, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *HostStyleSuite) TearDownSuite() {

	_ = DeleteBucket(svc, bucket)
}
