package helpers

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"flag"
	"fmt"
	"os"
	"bytes"
	"net/http"
	"log"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var Dflag *bool
var vflag *bool
var aws_access_key_id string
var aws_secret_access_key string
var my_region string = "mexico"	// value doesn't matter, but must be set
var my_endpoint string
var container string
var test_file string
var tflag int

func parse_opts() {
    errors := false
    flag.StringVar(&my_endpoint, "A", os.ExpandEnv("$MY_ENDPOINT"), "endpoint")
    flag.StringVar(&aws_access_key_id, "U", os.ExpandEnv("$AWS_ACCESS_KEY_ID"), "key_id")
    flag.StringVar(&aws_secret_access_key, "K", os.ExpandEnv("$AWS_SECRET_ACCESS_KEY"), "secret_key")
    Dflag = flag.Bool("D", false, "debug flag (print everything)")
    vflag = flag.Bool("v", false, "verbose (print details on errors)")
    flag.IntVar(&tflag, "t", 5, "test_number")
    flag.Parse()
    if (my_endpoint == "") {
	fmt.Fprintf(os.Stderr, "Must specify endpoint, either cmdline or $MY_ENDPOINT\n")
	errors = true
    }
    if (aws_access_key_id == "") {
	fmt.Fprintf(os.Stderr, "Must specify user, either cmdline or $AWS_ACCESS_KEY_ID\n")
	errors = true
    }
    if (aws_secret_access_key == "") {
	fmt.Fprintf(os.Stderr, "Must specify key, either cmdline or $AWS_SECRET_ACCESS_KEY\n")
	errors = true
    }
    if (flag.NArg() > 2) {
	fmt.Fprintf(os.Stderr, "Too many args, only (bucket local_file)\n")
	errors = true
    }
    if (flag.NArg() > 0) {
	container = flag.Args()[0]
    }
    if (flag.NArg() > 1) {
	test_file = flag.Args()[1]
    }
    if (container == "") {
	container = "load-test"
    }
    if (test_file == "") {
	test_file = "test.jpg"
    }
    if (errors) {
	flag.Usage()
	os.Exit(1)
    }
}


func Getcfg() *aws.Config {
	Connect()
	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad credentials: %s\n", err)
		return
	}
	cfg := aws.NewConfig().WithRegion(my_region).
		WithEndpoint(my_endpoint).
		WithDisableSSL(true).
		WithLogLevel(3).
		WithS3ForcePathStyle(true).
		WithCredentials(creds)

	return cfg
}

func GetDumycfg() *aws.Config {
	Connect()
	token := ""
	aws_secret_access_key := "sddgfhyfkiuloi;popo"
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad credentials: %s\n", err)
		return
	}
	cfg := aws.NewConfig().WithRegion(my_region).
		WithEndpoint(my_endpoint).
		WithDisableSSL(true).
		WithLogLevel(3).
		WithS3ForcePathStyle(true).
		WithCredentials(creds)

	return cfg
}

func WithIfNoneMatch(conditions ...string) request.Option {
    return func(r *request.Request) {
       for _, v := range conditions {
            r.HTTPRequest.Header.Add("If-None-Match", v)
       }
    }
}

sess, err := session.NewSession()
if err != nil {
	fmt.Fprintf(os.Stderr, "bad session=%v\n", err)
	return
}

svc := s3.New(sess, Getcfg())
uploader := s3manager.NewUploader(sess)
svclient := s3.New(sess, GetDumycfg())

func CreateBucket(bucket string) (error) {

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
    	Bucket: &bucket,
	})

	return err
}

func DeleteBucket(bucket) error{

	params := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := svc.DeleteBucket(params)

	// chini ya mazi wait till bucket is deleted.
	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	return err
}



func SetStringObject(bucket string, key string, content string) (error){

	_, err := svc.PutObject(&s3.PutObjectInput{
	    Body:   strings.NewReader(content),
	    Bucket: &bucket,
	    Key:    &key,
	})
	if err != nil {
	    log.Printf("Failed to write data", bucket, key, err)
	    return
	}

	return err
}

func SetStringObjectWithNoIfNoneMatch(bucket string, key string, content string) error {

	_, err := svc.PutObjectwithContext(ctx, &s3.PutObjectInput{
	    Bucket: aws.String(bucket),
	    Key:      aws.String(key),
	}, WithIfNoneMatch("etag")

	return err
}

func GetStringObject(bucket string, key string) (string)) {

	req, err := svc.GetObject(&s3.GetObjectInput{
	    Bucket: aws.String(bucket),
	    Key:    aws.String(key),
	})

	content, err := req.Presign(1 * time.Minute)
	
	if err != nil {
	    log.Printf("Failed to upload data to %s/%s, %s\n", bucket, key, err)
	    return
	}

	return content
}

func DeleteStringObject(bucket string, key string) error {

	params := &s3.DeleteObjectInput{
        Bucket: aws.String(bucket),
        Key : aws.String(key),
    }
	_, err := svc.DeleteObject(param)
	if err != nil {
	    log.Printf("Failed to delete %s/%s, %s\n", bucket, key)
	    return
	}

	return err
}

func ListStringObjects(bucket string) []string {

	result, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	for _, item in result.Contents{
		contents := item
	}

	return contents
}

func ListkeyswithDelimeter(bucket string, delimiter string) ([]string, []string) {

	params := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(10),
		Delimiter: aws.String(delimiter),
	}

	resp, err := svc.ListObjects(params})
	var keys []string
	var keys []string

	for _, value := range resp.Contents {
		for i := 0; i < len(value); i++ {
	        keys = Extend(*value.Key, i)
	        prefixes = Extend(*value.Prefix, i)
	    }
	}

	return  keys, prefixes

}

func DeleteStringObjects(bucket string) error {
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})

	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	num_objs := len(resp.Contents)
	var items s3.Delete
	var objs = make([]*s3.ObjectIdentifier, num_objs)

	for i, o := range resp.Contents {
		objs[i] = &s3.ObjectIdentifier{Key: aws.String(*o.Key)}
	}
	items.SetObjects(objs)
	_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{Bucket: &bucket, Delete: &items})

	if err != nil {
		exitErrorf("Unable to delete objects from bucket %q, %v", bucket, err)
	}

	return err	
}

func UploadFile(bucket string, file_to_upload string) (error, *s3.PutObjectInput) {

	file, err := os.Open(file_to_upload) 
	if err != nil { 
	    exitErrorf("err opening file: %s", err) 
	} 
	defer file.Close() 
	fileInfo, _ := file.Stat() 
	size := fileInfo.Size() 
	buffer := make([]byte, size) // read file content to buffer 
	file.Read(buffer) 
	fileBytes := bytes.NewReader(buffer) 
	fileType := http.DetectContentType(buffer) 
	path := "/home/joan" + file.Name() 

	params := &s3manager.UploadInput{ 
		Bucket: aws.String(bucket), 
		Key: aws.String(path), 
		ACL: aws.String("public-read"),
	    Body: fileBytes, 
	    ContentLength: aws.Int64(size), 
	    ContentType: aws.String(fileType), 
	} 

	resp, err := uploader.Upload(params) 
	return resp, err
}

func Postwithwrongkey(bucket string, key string) (*s3.PutObjectInput, error) {

	svc = svclient
	req, err := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String("testBucket"),
		Key: aws.String("testKey"),
		ACL: aws.String("private"),
	})

	return req, err
}