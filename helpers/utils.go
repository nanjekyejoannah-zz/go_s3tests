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


func Getcfg() *Config {
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

sess, err := session.NewSession()
if err != nil {
	fmt.Fprintf(os.Stderr, "bad session=%v\n", err)
	return
}

svc := s3.New(sess, utils.Getcfg())

func CreateBucket(bucket string){

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
    	Bucket: &bucket,
	})

	if err != nil {
	    log.Println("Bucket creation failed", err)
	    return
	}
}

func DeleteBucket(bucket){

	params := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := svc.DeleteBucket(params)
	if err != nil {
	    log.Println("Bucket delete failed", err)
	    return
	}
}



func WriteObject(bucket string, key string, content string){

	_, err := svc.PutObject(&s3.PutObjectInput{
	    Body:   strings.NewReader(content),
	    Bucket: &bucket,
	    Key:    &key,
	})
	if err != nil {
	    log.Printf("Failed to write data", bucket, key, err)
	    return
	}
}

func ReadObject(bucket string, key string) string {

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

func DeleteObject(bucket string, key string){

	params := &s3.DeleteObjectInput{
        Bucket: aws.String(bucket),
        Key : aws.String(key),
    }
	_, err := svc.DeleteObject(param)
	if err != nil {
	    log.Printf("Failed to delete %s/%s, %s\n", bucket, key)
	    return
	}
}
