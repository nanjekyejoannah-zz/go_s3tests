package helpers

// see http://tracker.ceph.com/issues/19627
package main

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

// This is a test for exploring Go->s3->ceph
func main() {
    parse_opts()
	// XXX these should also be configurable.

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
	fmt.Printf("cfg=%v\n", cfg)
	sess, err := session.NewSession() // There is a session.Must() for convenience
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad session=%v\n", err)
		return
	}
	fmt.Printf("sess=%v\n", sess)
	svc := s3.New(sess, cfg)
	fmt.Printf("svc=%v\n", svc)

	switch tflag {
	case 1:

		/// FIXME uploading with an uploader, failed talking to lab and to docker
		// 2017/04/12 14:56:57 Unable to upload "test.jpg" to "load-test", MissingRegion: could not find region configuration
		file, err := os.Open(test_file)
		if err != nil {
			log.Fatalf("Unable to open file %q, %v", err)
			break
		}
		defer file.Close()

//		uploader := s3manager.NewUploader(sess)
		uploader := s3manager.NewUploaderWithClient(svc)
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(container),
			Key:    aws.String(test_file),
			Body:   file,
		})
		fmt.Printf("uploader=%v\n", uploader)
		if err != nil {
			// Print the error and exit.
			fmt.Fprintf(os.Stderr, "Unable to upload %q to %q, %v\n", test_file, container, err)
			break
		}

		break

	case 2:
		// Wait until container is created before finishing
		fmt.Printf("Waiting for container %q to be created...\n", container)
		err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: aws.String(container),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occurred while waiting for container to be created, %v\n", err)
			break
		}
		fmt.Printf("Bucket %q successfully created\n", container)

		break

	case 3:
		// list buckets, works in lab
		// says failed to list buckets, NoSuchBucket with docker
		result, err := svc.ListBuckets(nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to list buckets, %v\n", err)
			break
		}
		for i, b := range result.Buckets {
			//fmt.Printf("* %s created on %s\n",
			//	aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
			fmt.Printf("container %d = %v\n", i, b)
		}

		break

	case 4:
		// list objects
		i := 0
		err = svc.ListObjectsPages(&s3.ListObjectsInput{
			Bucket: &container,
		}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			fmt.Println("Page,", i)
			i++

			for _, obj := range p.Contents {
				fmt.Println("Object:", *obj.Key)
			}
			return true
		})
		if err != nil {
			fmt.Println("failed to list objects", err)
			break
		}

		break
	case 5:
		file, err := os.Open("test.jpg")
		if err != nil {
			fmt.Fprintf(os.Stderr, "err opening file: %s\n", err)
			break
		}
		defer file.Close()
		fileInfo, _ := file.Stat()
		size := fileInfo.Size()
		buffer := make([]byte, size) // read file content to buffer

		file.Read(buffer)
		fileBytes := bytes.NewReader(buffer)
		fileType := http.DetectContentType(buffer)
		params := &s3.PutObjectInput{
			Bucket:        aws.String(container),
			Key:           aws.String("test.jpg"),
			Body:          fileBytes,
			ContentLength: aws.Int64(size),
			ContentType:   aws.String(fileType),
		}
		resp, err := svc.PutObject(params)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad response: %s\n", err)
			break
		}
		fmt.Printf("response %s\n", awsutil.StringValue(resp))
	}
}
