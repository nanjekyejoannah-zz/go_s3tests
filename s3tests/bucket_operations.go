package s3tests

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"bytes"
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type Configuration struct {
	RGW          rgwConfig
	JwtKey       string `yaml:"default_key"`
	IgnoreString string `yaml:"-"`
}

type rgwConfig struct {
	rgw_access_key_id     string `yaml:"key"`
	rgw_secret_access_key string `yaml:"secret"`
	bucket                string `yaml:"bucket"`
	my_end_point          string `yaml:"endpoint"`
	my_region             string `yaml:"region"`
}

func LoadConfig(path string) Configuration {
	file, err := ioutil.ReadFile(path)
	var config Configuration

	err = yaml.Unmarshal(file, &config)
	if err != nil {
		fmt.Printf("Config Parse Error: ")
	}

	return config
}

var config = LoadConfig("config.yaml")

var creds = credentials.NewStaticCredentials("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==", "")

var cfg = aws.NewConfig().WithRegion("mexico").
	WithEndpoint("http://localhost:8000/").
	WithDisableSSL(true).
	WithLogLevel(3).
	WithS3ForcePathStyle(true).
	WithCredentials(creds)

var sess = session.Must(session.NewSession())
var svc = s3.New(sess, cfg)
var uploader = s3manager.NewUploader(sess)
var downloader = s3manager.NewDownloader(sess)

func SliceContains(s []string, e string) bool {
	resp := false

	for _, a := range s {
		if a == e {
			resp = true
			break
		} else {
			resp = false
			break
		}
	}

	return resp
}

func CreateBucket(bucket string) error {

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

func CreateBucketAndKey(bucket string, key string) error {

	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	return err
}

func PutObjectToBucket(bucket string, key string, content string) error {

	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: &bucket,
		Key:    &key,
	})

	return err
}

func DeleteBucket(bucket string) error {

	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}

func ListBuckets() ([]string, error) {

	var bukts []string

	result, err := svc.ListBuckets(nil)

	for _, bucket := range result.Buckets {
		bukts = append(bukts, aws.StringValue(bucket.Name))
	}
	return bukts, err
}

func ListObjects(bucket string) ([]*s3.Object, error) {

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	return resp.Contents, err
}

func GetObject(bucket string, key string) (string, error) {

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

func DeleteObject(bucket string, key string) error {

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String("Bucketname"),
		Key:    aws.String("ObjectKey"),
	})

	return err
}

func DeleteObjects(bucket string) error {

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

func GetKeys(bucket string) ([]string, error) {
	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return keys, err
}

func GetKeysWithMaxKeys(bucket string, maxkeys int64) ([]string, error) {
	var keys []string

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(maxkeys),
	})

	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return keys, err
}

func GetKeysWithMaxKeysAndMarker(bucket string, maxkeys int64, marker string) ([]string, error) {

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

func CopyObject(other string, source string, item string) error {

	_, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(other),
		CopySource: aws.String(source),
		Key:        aws.String(item)})

	return err
}

func SinglFileUpload(bucket string, filename string) error {

	file, _ := os.Open(filename)
	defer file.Close()

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   file,
	})

	return err
}

func GeneratePresignedUrlGetObject(bucket string, key string) (string, error) {

	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	urlStr, err := req.Presign(15 * time.Minute)

	return urlStr, err
}

