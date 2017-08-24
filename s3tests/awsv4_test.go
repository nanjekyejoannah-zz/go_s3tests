package s3test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"

	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"time"

	. "../Utilities"
)

func (suite *S3Suite) TestPresignRequest() {

	assert := suite
	region := viper.GetString("s3main.region")
	req, body := SetupRequest("S3", region, "{}")

	signer := SetupSigner(Creds)
	signer.Presign(req, body, "s3", region, 300*time.Second, time.Unix(0, 0))

	qry := req.URL.Query()
	assert.Equal("5cb339c731e21d25810fea3eeb399bd33a4ea75631d9f9e7f8a8c65d271b7dad", qry.Get("X-Amz-Signature"))
	assert.Equal("0555b35654ad1656d804/19700101/us-east-1/s3/aws4_request", qry.Get("X-Amz-Credential"))
	assert.Equal("content-length;content-type;host;x-amz-meta-other-header;x-amz-meta-other-header_with_underscore", qry.Get("X-Amz-SignedHeaders"))
	assert.Equal("19700101T000000Z", qry.Get("X-Amz-Date"))
}

func (suite *S3Suite) TestSignRequest() {

	assert := suite
	region := viper.GetString("s3main.region")
	req, body := SetupRequest("S3", region, "{}")
	expectedauth := "AWS4-HMAC-SHA256 Credential=0555b35654ad1656d804/19700101/us-east-1/s3/aws4_request, SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-meta-other-header;x-amz-meta-other-header_with_underscore;x-amz-target, Signature=605bf71358a549b8aa9461aeb0944908a62395efdf4bb5fc8bdb47b48147a426"
	signer := SetupSigner(Creds)
	signer.Sign(req, body, "s3", region, time.Unix(0, 0))

	qry := req.Header
	assert.Equal(expectedauth, qry.Get("Authorization"))
	assert.Equal("19700101T000000Z", qry.Get("X-Amz-Date"))
}

func (suite *S3Suite) TestSignBody() {

	assert := suite
	region := viper.GetString("s3main.region")
	req, body := SetupRequest("S3", region, "yello")

	signer := SetupSigner(Creds)
	signer.Sign(req, body, "s3", region, time.Now())

	hash := req.Header.Get("X-Amz-Content-Sha256")
	assert.Equal("0e6807fb3a06ab2a6ee35df3d89365b2af1266eb390e9e687e9a500de32571bd", hash)
}

func (suite *S3Suite) TestPresignEmptyBody() {

	assert := suite
	region := viper.GetString("s3main.region")
	req, body := SetupRequest("S3", region, "yello")

	signer := SetupSigner(Creds)
	signer.Presign(req, body, "s3", region, 5*time.Minute, time.Now())

	hash := req.Header.Get("X-Amz-Content-Sha256")
	assert.Equal("UNSIGNED-PAYLOAD", hash)
}

func (suite *S3Suite) TestSignUnsignedpayload() {

	assert := suite
	region := viper.GetString("s3main.region")
	req, body := SetupRequest("S3", region, "yello")

	signer := SetupSigner(Creds)
	signer.Presign(req, body, "s3", region, 5*time.Minute, time.Now())

	hash := req.Header.Get("X-Amz-Content-Sha256")
	assert.Equal("UNSIGNED-PAYLOAD", hash)
}

func (suite *S3Suite) TestSignWithRequestBody() {

	assert := suite
	signer := v4.NewSigner(Creds)

	expectBody := []byte("abc123")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		assert.Nil(err)
		assert.Equal(expectBody, b)
		w.WriteHeader(http.StatusOK)
	}))

	req, err := http.NewRequest("POST", server.URL, nil)

	_, err = signer.Sign(req, bytes.NewReader(expectBody), "service", "region", time.Now())
	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *S3Suite) TestSignWithRequestBodyOverwrite() {

	assert := suite
	signer := v4.NewSigner(Creds)

	var expectBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		assert.Nil(err)
		assert.Equal(len(expectBody), len(b))
		w.WriteHeader(http.StatusOK)
	}))

	req, err := http.NewRequest("GET", server.URL, strings.NewReader("invalid body"))

	_, err = signer.Sign(req, nil, "service", "region", time.Now())
	req.ContentLength = 0

	assert.Nil(err)

	resp, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *S3Suite) TestSignWithBodyReplaceRequestBody() {

	assert := suite
	region := viper.GetString("s3main.region")

	req, seekerBody := SetupRequest("S3", region, "{}")
	req.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))

	s := v4.NewSigner(Creds)
	origBody := req.Body

	_, err := s.Sign(req, seekerBody, "s3", "mexico", time.Now())
	assert.Nil(err)
	assert.NotEqual(req.Body, origBody)
	assert.NotNil(req.Body)
}

func (suite *S3Suite) TestSignWithBodyNoReplaceRequestBody() {

	assert := suite
	region := viper.GetString("s3main.region")

	req, seekerBody := SetupRequest("S3", region, "{}")
	req.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))

	s := v4.NewSigner(Creds, func(signer *v4.Signer) {
		signer.DisableRequestBodyOverwrite = true
	})

	origBody := req.Body

	_, err := s.Sign(req, seekerBody, "s3", "mexico", time.Now())
	assert.Nil(err)
	assert.Equal(req.Body, origBody)
}

func (suite *S3Suite) TestPresignHandler() {

	assert := suite
	req, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket:             aws.String("bucket"),
		Key:                aws.String("key"),
		ContentDisposition: aws.String("a+b c$d"),
		ACL:                aws.String("public-read"),
	})

	req.Time = time.Unix(0, 0)
	urlstr, err := req.Presign(5 * time.Minute)

	assert.Nil(err)

	expectedHost := viper.GetString("s3main.endpoint")
	expectedDate := "19700101T000000Z"
	expectedHeaders := "content-disposition;host;x-amz-acl"
	expectedSig := "74dc17e5958f1304eaf6397f1d3078d55b533a076475024622935aa613666ec2"
	expectedCred := "0555b35654ad1656d804/19700101/us-east-1/s3/aws4_request"

	u, _ := url.Parse(urlstr)
	urlQ := u.Query()
	assert.Equal(expectedHost, u.Host)
	assert.Equal(expectedSig, urlQ.Get("X-Amz-Signature"))
	assert.Equal(expectedCred, urlQ.Get("X-Amz-Credential"))
	assert.Equal(expectedHeaders, urlQ.Get("X-Amz-SignedHeaders"))
	assert.Equal(expectedDate, urlQ.Get("X-Amz-Date"))
	assert.Equal("300", urlQ.Get("X-Amz-Expires"))

	assert.NotContains(urlstr, "+") // + encoded as %20
}

func (suite *S3Suite) TestStandaloneSignCustomURIEscape() {

	assert := suite
	var expectSig = "AWS4-HMAC-SHA256 Credential=0555b35654ad1656d804/19700101/us-east-1/es/aws4_request, SignedHeaders=host;x-amz-date, Signature=c79ab70ccf1424132da60f559db2cd3e1502b0d002ba2a72940facd380742b1d"
	signer := v4.NewSigner(Creds, func(s *v4.Signer) {
		s.DisableURIPathEscaping = true
	})

	host := "https://subdomain.us-east-1.es.amazonaws.com"
	req, err := http.NewRequest("GET", host, nil)
	assert.Nil(err)

	req.URL.Path = `/log-*/_search`
	req.URL.Opaque = "//subdomain.us-east-1.es.amazonaws.com/log-%2A/_search"

	_, err = signer.Sign(req, nil, "es", "us-east-1", time.Unix(0, 0))
	assert.Nil(err)

	actual := req.Header.Get("Authorization")
	assert.Equal(expectSig, actual)
}
