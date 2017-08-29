package helpers

import ( 

  "testing"
  "github.com/stretchr/testify/assert"

  "github.com/spf13/viper"
)

func TestLoadConfig (t *testing.T) {

	assert := assert.New(t)

	assert.Equal(LoadConfig(), nil)

	assert.Equal(viper.GetString("s3main.access_key"), "0555b35654ad1656d804")
	assert.Equal(viper.GetString("s3main.access_secret"), "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==")
	assert.Equal(viper.GetString("s3main.region"), "us-east-1")
}

