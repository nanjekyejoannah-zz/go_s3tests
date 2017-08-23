package main

import (

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	
	"fmt"
	"github.com/spf13/viper"

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

var Creds = credentials.NewStaticCredentials("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==", "")

var cfg = aws.NewConfig().WithRegion("us-east-1").
	WithEndpoint("http://localhost:8000/").
	WithDisableSSL(true).
	WithLogLevel(3).
	WithS3ForcePathStyle(true).
	WithCredentials(Creds)

var sess = session.Must(session.NewSession())
var svc = s3.New(sess, cfg)

func main() {


	result, err := svc.ListBuckets(nil)

	if err != nil {

        fmt.Printf("Error Listing buckets, %v", err)
    }

	for _, b := range result.Buckets {

		fmt.Println("............Buckets.................")
	    fmt.Printf("* %s created on %s\n", aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))

	}

}
