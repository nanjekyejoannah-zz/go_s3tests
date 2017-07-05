package s3test

import (

	"github.com/stretchr/testify/suite"
	"testing"

	. "../Utilities"
)

var svc = GetConn()


type S3Suite struct {
    suite.Suite
}

func (suite *S3Suite) SetupTest() {
    
}

func TestSuite(t *testing.T) {

    //suite.Run(t, new(S3Suite))
    suite.Run(t, new(HostStyleSuite))
}

func (suite *S3Suite) TearDownTest() {
	
	DeletePrefixedBuckets(svc)  
}