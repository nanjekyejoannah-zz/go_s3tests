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

var Creds = credentials.NewStaticCredentials(viper.GetString("s3main.access_key"), viper.GetString("s3main.access_secret"), "")

var cfg = aws.NewConfig().WithRegion(viper.GetString("s3main.region")).
	WithEndpoint(viper.GetString("s3main.endpoint")).
	WithDisableSSL(true).
	WithLogLevel(3).
	WithS3ForcePathStyle(true).
	WithCredentials(Creds)

var sess = session.Must(session.NewSession())
var svc = s3.New(sess, cfg)

func GetConn() *s3.S3 {

	return svc
}


func main() {

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
        Bucket: aws.String("bkt1"),
    })

    if err != nil {
        fmt.Printf("go errors creating buckets")
    }

}
