// see http://tracker.ceph.com/issues/19627
package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"flag"
	"fmt"
	"os"
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

		//create bucket
		_, err = svc.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String("bucket1"),
		})

		if err != nil {
                fmt.Fprintf(os.Stderr, "failed to create buckets, %v\n", err)
        }

        // list buckets
        result, err := svc.ListBuckets(nil)
        if err != nil {
                fmt.Fprintf(os.Stderr, "failed to list buckets, %v\n", err)
        }
        for i, b := range result.Buckets {
                //fmt.Printf("* %s created on %s\n",
                //	aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
                fmt.Printf("container %d = %v\n", i, b)
                break
        }

}
