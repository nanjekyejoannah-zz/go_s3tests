package helpers

import (

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/signer/v4"

	"bytes"
	"golang.org/x/net/context"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"strings"
	"os"
	"time"
	"net/http"
)

func LoadConfig() error {

	viper.SetConfigName("config")  
  	viper.AddConfigPath("../")

  	err := viper.ReadInConfig() 
  	if err != nil {
    	fmt.Println("Config file not found...")
  	}

  	return err
}

var err = LoadConfig()

var Creds = credentials.NewStaticCredentials(viper.GetString("s3main.access_key"), viper.GetString("s3main.access_secret"), "")

var cfg = aws.NewConfig().WithRegion(viper.GetString("s3main.region")).
	WithEndpoint(viper.GetString("s3main.endpoint")).
	WithDisableSSL(true).
	WithLogLevel(3).
	WithS3ForcePathStyle(true).
	WithCredentials(Creds)

var sess = session.Must(session.NewSession())
var svc = s3.New(sess, cfg)
var uploader = s3manager.NewUploader(sess)
var downloader = s3manager.NewDownloader(sess)

func GetConn() (*s3.S3) {

	return svc	
}

func WithIfNoneMatch(conditions ...string) request.Option {
    return func(r *request.Request) {
       for _, v := range conditions {
            r.HTTPRequest.Header.Add("If-None-Match", v)
       }
    }
}

func WithIfMatch(conditions ...string) request.Option {
    return func(r *request.Request) {
       for _, v := range conditions {
            r.HTTPRequest.Header.Add("If-Match", v)
       }
    }
}

func AddHeaders(conditions map[string]string) request.Option {

    return func(r *request.Request) {
       for k, v := range conditions {
            r.HTTPRequest.Header.Add(k,v)
       }
    }
}

func CreateBucket(svc *s3.S3, bucket string) error {

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

func PutObjectToBucket(svc *s3.S3, bucket string, key string, content string) error { //deprecated

	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: &bucket,
		Key:    &key,
	})

	return err
}

func CreateObjects(svc *s3.S3, bucket string, objects map[string]string) error { // for this

	for key, content := range objects {

		_, err := svc.PutObject(&s3.PutObjectInput{
			Body:   strings.NewReader(content),
			Bucket: &bucket,
			Key:    &key,
		})

		err = err
	}

	return err
}

func DeleteBucket(svc *s3.S3, bucket string) error {

	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

func ListBuckets(svc *s3.S3) ([]string, error) {

	var bukts []string

	result, err := svc.ListBuckets(nil)

	for _, bucket := range result.Buckets {
		bukts = append(bukts, aws.StringValue(bucket.Name))
	}
	return bukts, err
}

func ListObjects(svc *s3.S3, bucket string) ([]*s3.Object, error) {

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	return resp.Contents, err
}

func GetObjects(svc *s3.S3, bucket string) (*s3.ListObjectsOutput, error) {

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	return resp, err
}

func ListObjectsWithDelimeterAndPrefix(svc *s3.S3, bucket string, prefix string, delimiter string) (*s3.ListObjectsOutput, []string, []string, error) {

	keys := []string {}
	prefixes := []string {}

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		Delimiter: aws.String(delimiter),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	for _, commonPrefix := range resp.CommonPrefixes {
        prefixes = append(prefixes, *commonPrefix.Prefix)
    }

	return resp, keys, prefixes, err
}

func ListObjectsWithPrefix(svc *s3.S3, bucket string, prefix string) (*s3.ListObjectsOutput, []string, []string, error) {

	keys := []string {}
	prefixes := []string {}

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	for _, commonPrefix := range resp.CommonPrefixes {
        prefixes = append(prefixes, *commonPrefix.Prefix)
    }

	return resp, keys, prefixes, err
}

func ListObjectsWithDelimiter(svc *s3.S3, bucket string, delimiter string) (*s3.ListObjectsOutput, []string, []string, error) {

	keys := []string {}
	prefixes := []string {}

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Delimiter: aws.String(delimiter),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

    for _, commonPrefix := range resp.CommonPrefixes {
        prefixes = append(prefixes, *commonPrefix.Prefix)
    }

	return resp, keys, prefixes, err
}


func GetObject(svc *s3.S3, bucket string, key string) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}

func GetObjectWithRange(svc *s3.S3, bucket string, key string, range_value string) (*s3.GetObjectOutput, string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), 
					Key: aws.String(key), Range: aws.String(range_value) })

	var data string
	var errr error
	var resp *s3.GetObjectOutput

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return results, "", err
		}

		byteArray := buf.Bytes()

		resp, data, errr = results, string(byteArray[:]), err

	} else {

		resp, data, errr = results, "", err
	}

	return resp, data, errr
}

func DeleteObject(svc *s3.S3, bucket string, key string) error {

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String("Bucketname"),
		Key:    aws.String("ObjectKey"),
	})

	return err
}

func DeleteObjects(svc *s3.S3, bucket string) error {

	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})

	num_objs := len(resp.Contents)
	var items s3.Delete
	var objs = make([]*s3.ObjectIdentifier, num_objs)

	for i, o := range resp.Contents {
		objs[i] = &s3.ObjectIdentifier{Key: aws.String(*o.Key)}
	}

	items.SetObjects(objs)
	_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{Bucket: &bucket, Delete: &items})

	return err
}

func GetKeys(svc *s3.S3, bucket string) (*s3.ListObjectsOutput, []string, error) {
	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return resp, keys, err
}

func GetKeysWithMaxKeys(svc *s3.S3, bucket string, maxkeys int64) (*s3.ListObjectsOutput, []string, error) {
	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(maxkeys),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return resp, keys, err
}

func GetKeysWithMarker(svc *s3.S3, bucket string, marker string) (*s3.ListObjectsOutput, []string, error) {
	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		Marker:  aws.String(marker),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return resp, keys, err
}

func GetKeysWithMaxKeysAndMarker(svc *s3.S3, bucket string, maxkeys int64, marker string) ([]string, error) {

	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String("bucket"),
		MaxKeys: aws.Int64(maxkeys),
		Marker:  aws.String(marker),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return keys, err
}

func CopyObject(svc *s3.S3, other string, source string, item string) error {

	_, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(other),
		CopySource: aws.String(source),
		Key:        aws.String(item)})

	return err
}

func GeneratePresignedUrlGetObject(svc *s3.S3, bucket string, key string) (string, error) {

	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	urlStr, err := req.Presign(15 * time.Minute)

	return urlStr, err
}

func DeletePrefixedBuckets(svc *s3.S3){

  buckets, err := svc.ListBuckets(&s3.ListBucketsInput{})

  if err != nil {
    panic(fmt.Sprintf("failed to list buckets, %v", err))
  }

  for _, b := range buckets.Buckets {
    bucket := aws.StringValue(b.Name)

    if !strings.HasPrefix(bucket, prefix) {
      continue
    }
    
    if err := DeleteObjects(svc, bucket); err != nil {
      fmt.Fprintf(os.Stderr, "failed to delete objects %q, %v", bucket, err)
    }

    if err := DeleteBucket(svc, bucket); err != nil {
      fmt.Fprintf(os.Stderr, "failed to delete bucket %q, %v", bucket, err)
    }
  }


}

func EncryptionSSECustomerWrite (svc *s3.S3, filesize int) (string, string, error) {

	data :=  strings.Repeat("A", filesize)
	key := "testobj"
	bucket := GetBucketName()
	sse := []string{"AES256", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=","DWygnHRtgiJ77HCm+1rvHw=="}

	err := CreateBucket(svc, bucket)

	err = WriteSSECEcrypted(svc, bucket, key, data, sse)

	rdata, _ := ReadSSECEcrypted(svc, bucket, key, sse)

	return rdata, data, err
}

func SSEKMSkeyIdCustomerWrite(svc *s3.S3, filesize int) (string, string, error) {

	data :=  strings.Repeat("A", filesize)
	key := "testobj"
	bucket := GetBucketName()
	sse := viper.GetString("s3main.SSE")
	kmskeyid := viper.GetString("s3main.kmskeyid")

	err := CreateBucket(svc, bucket)

	err = WriteSSEKMSkeyId(svc, bucket, key, data, sse, kmskeyid)

	rdata, _ := GetObject(svc, bucket, key)

	return rdata, data, err
}

func SSEKMSCustomerWrite(svc *s3.S3, filesize int) (string, string, error) {

	data :=  strings.Repeat("A", filesize)
	key := "testobj"
	bucket := GetBucketName()
	sse := viper.GetString("s3main.SSE")

	err := CreateBucket(svc, bucket)

	err = WriteSSEKMS(svc, bucket, key, data, sse)

	rdata, _ := GetObject(svc, bucket, key)

	return rdata, data, err
}


func WriteSSECEcrypted(svc *s3.S3, bucket string, key string, content string, sse []string) error { //deprecated

	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: &bucket,
		Key:    &key,
		SSECustomerAlgorithm: &sse[0],
		SSECustomerKey: &sse[1],
		SSECustomerKeyMD5: &sse[2],
	})

	return err
}

func ReadSSECEcrypted(svc *s3.S3, bucket string, key string, sse []string) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket), 
		Key: aws.String(key), 
		SSECustomerAlgorithm: &sse[0],
		SSECustomerKey: &sse[1],
		SSECustomerKeyMD5: &sse[2],
	})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}

func WriteSSEKMS(svc *s3.S3, bucket string, key string, content string, sse string) error { //deprecated

	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: &bucket,
		Key:    &key,
		ServerSideEncryption: &sse,
	})

	return err
}

func WriteSSEKMSkeyId(svc *s3.S3, bucket string, key string, content string, sse string, kmskeyid string) error { //deprecated

	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: &bucket,
		Key:    &key,
		ServerSideEncryption: &sse,
		SSEKMSKeyId: &kmskeyid,
	})

	return err
}

func GetSetMetadata (metadata map[string]*string) map[string]*string {

	bucket := GetBucketName()
	objects := map[string]string{ "key1": "echo",}
	key := objects["key1"]

	_ = CreateBucket(svc, bucket)
	_ = CreateObjects(svc, bucket, objects)

	resp, _ := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	resp.SetMetadata(metadata)

	return resp.Metadata
}

func GetObjectWithIfMatch(svc *s3.S3, bucket string, key string, condition string) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), IfMatch: aws.String(condition)})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}

func GetObjectWithIfNoneMatch(svc *s3.S3, bucket string, key string, condition string) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), IfNoneMatch: aws.String(condition)})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}

func GetObjectWithIfModifiedSince(svc *s3.S3, bucket string, key string, time time.Time) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), IfModifiedSince: &time})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}

func GetObjectWithIfUnModifiedSince(svc *s3.S3, bucket string, key string, time time.Time) (string, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), IfUnmodifiedSince: &time})

	var resp string
	var errr error

	if err == nil {

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, results.Body); err != nil {
			return "", err
		}

		byteArray := buf.Bytes()

		resp, errr = string(byteArray[:]), err

	} else {

		resp, errr = "", err
	}

	return resp, errr
}


func GetObj(svc *s3.S3, bucket string, key string) (*s3.GetObjectOutput, error) {

	results, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})

	return results, err
}

func PutObjectWithIfMatch (svc *s3.S3, bucket string, key string, content string, tag string) error {

	data, err := GetObject(svc, bucket, key)

	if data != "" {

		fmt.Sprintf("some data, %v", data)
	}

	if err == nil{

		ctx := context.Background()
		ctx, _ = context.WithTimeout(ctx, time.Minute)

		_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		    Bucket: aws.String(bucket),
		    Key:    aws.String(key),
		    Body:   strings.NewReader(content),
		}, WithIfNoneMatch(tag))

	}

	return err
}

func PutObjectWithIfNoneMatch (svc *s3.S3, bucket string, key string, content string, tag string) error {

	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Minute)

	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
	    Bucket: aws.String(bucket),
	    Key:    aws.String(key),
	    Body:   strings.NewReader(content),
	}, WithIfNoneMatch(tag))

	return err
}

func AbortMultiPartUpload(svc *s3.S3, bucket string, key string, uploadid string) (*s3.AbortMultipartUploadOutput, error) {

	params := &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket),
	    Key:    aws.String(key),
	    UploadId: aws.String(uploadid),
	}

	result, err := svc.AbortMultipartUpload(params)

	return result, err
}

func AbortMultiPartUploadInvalid(svc *s3.S3, bucket string, key string, uploadid string) (*s3.AbortMultipartUploadOutput, error) {

	params := &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket),
	    Key:    aws.String(key),
	}

	result, err := svc.AbortMultipartUpload(params)

	return result, err
}

func InitiateMultipartUpload(svc *s3.S3, bucket string, key string) (*s3.CreateMultipartUploadOutput, error){

	input := &s3.CreateMultipartUploadInput{
    	Bucket: aws.String(bucket),
    	Key:    aws.String(key),
	}

	result, err := svc.CreateMultipartUpload(input)

	return result, err

}

func UploadCopyPart (svc *s3.S3, bucket string, key string, source string, uploadid string, partnumber int64 ) (*s3.UploadPartCopyOutput, error){

	input := &s3.UploadPartCopyInput{
	    Bucket:     aws.String(bucket),
	    CopySource: aws.String(source),
	    Key:        aws.String(key),
	    PartNumber: aws.Int64(partnumber),
	    UploadId:   aws.String(uploadid),
	}

	result, err := svc.UploadPartCopy(input)

	return result, err
}

func CompleteMultiUpload(svc *s3.S3, bucket string, key string, partNum int64, uploadid string, etag string )(*s3.CompleteMultipartUploadOutput, error){

	input := &s3.CompleteMultipartUploadInput{
	    Bucket: aws.String(bucket),
	    Key:    aws.String(key),
	    MultipartUpload: &s3.CompletedMultipartUpload{
	        Parts: []*s3.CompletedPart{
		            {
		                ETag:       aws.String(etag),
		                PartNumber: aws.Int64(partNum),
		            },
		        },
		    },
		UploadId: aws.String(uploadid),
	}

	result, err := svc.CompleteMultipartUpload(input)

	return result, err
}

func Listparts (svc *s3.S3, bucket string, key string, uploadid string)(*s3.ListPartsOutput, error){

	input := &s3.ListPartsInput{
	    Bucket:   aws.String(bucket),
	    Key:      aws.String(key),
	    UploadId: aws.String(uploadid),
	}

	result, err := svc.ListParts(input)

	return result, err
}

func Uploadpart (svc *s3.S3, bucket string, key string, uploadid string, content string, partNum int64)(*s3.UploadPartOutput, error){

	input := &s3.UploadPartInput{
	    Body:       aws.ReadSeekCloser(strings.NewReader(content)),
	    Bucket:     aws.String(bucket),
	    Key:        aws.String(key),
	    PartNumber: aws.Int64(partNum),
	    UploadId:   aws.String(uploadid),
	}

	result, err := svc.UploadPart(input)

	return result, err
}

func SetupObjectWithHeader(svc *s3.S3, bucket string, key string, content string, headers map[string]string) (error) {

	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Minute)

	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	}, AddHeaders(headers))

	return err
}

func SetupBucketWithHeader(svc *s3.S3, bucket string, headers map[string]string) (error) {

	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Minute)

	_, err = svc.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}, AddHeaders(headers))

	return err
}

func CreateBucketWithHeader(svc *s3.S3, bucket string, headers map[string]string) error {

	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Minute)

	_, err := svc.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}, AddHeaders(headers))

	return err
}

func SetLifecycle(svc *s3.S3, bucket , id , status, md5 string) (*s3.PutBucketLifecycleConfigurationOutput, error) {

	input := &s3.PutBucketLifecycleConfigurationInput{
	    Bucket: aws.String(bucket),
	    LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
	        Rules: []*s3.LifecycleRule{
	            {
	                ID:     aws.String(id),
	                Status: aws.String(status),
	            },
	        },
	    },
	}
	req, resp := svc.PutBucketLifecycleConfigurationRequest(input)
	req.HTTPRequest.Header.Set("Content-Md5", string(md5))

	err := req.Send()

	return resp, err
}

func GetLifecycle(svc *s3.S3, bucket string) (*s3.GetBucketLifecycleConfigurationOutput, error) {

	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Minute)

	input := &s3.GetBucketLifecycleConfigurationInput{
	    Bucket: aws.String(bucket),
	}

	result, err := svc.GetBucketLifecycleConfigurationWithContext(ctx, input)

	return result, err
}

func SetACL (svc *s3.S3, bucket string, acl string)(*s3.PutBucketAclOutput, error){

	req, resp := svc.PutBucketAclRequest(&s3.PutBucketAclInput{
		Bucket: aws.String(bucket),
		ACL: 	aws.String(acl),
	})

	err := req.Send()

	return resp, err
}

func SetupRequest(serviceName, region, body string) (*http.Request, io.ReadSeeker) {

	endpoint := "https://" + serviceName + "." + region + "." + viper.GetString("s3main.endpoint")
	reader := strings.NewReader(body)
	req, _ := http.NewRequest("POST", endpoint, reader)
	req.Header.Add("X-Amz-Target", "prefix.Operation")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")
	req.Header.Add("Content-Length", string(len(body)))
	req.Header.Add("X-Amz-Meta-Other-Header", "some-value=!@#$%^&* (+)")
	req.Header.Add("X-Amz-Meta-Other-Header_With_Underscore", "some-value=!@#$%^&* (+)")
	req.Header.Add("X-amz-Meta-Other-Header_With_Underscore", "some-value=!@#$%^&* (+)")

	return req, reader
}

func SetupRawRequest(proto, method, url, body string) (*http.Request, io.ReadSeeker) {

	endpoint := proto + "://" + url
	reader := strings.NewReader(body)
	req, _ := http.NewRequest(method, endpoint, reader)
	req.Header.Add("X-Amz-Target", "prefix.Operation")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")
	req.Header.Add("Content-Length", string(len(body)))
	req.Header.Add("X-Amz-Meta-Other-Header", "some-value=!@#$%^&* (+)")
	req.Header.Add("X-Amz-Meta-Other-Header_With_Underscore", "some-value=!@#$%^&* (+)")
	req.Header.Add("X-amz-Meta-Other-Header_With_Underscore", "some-value=!@#$%^&* (+)")

	return req, reader
}

func SetupSigner(creds *credentials.Credentials) v4.Signer {

	return v4.Signer{
		Credentials: creds,
	}
}