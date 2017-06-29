package helpers

import (  
  "math/rand"
  "time"

  "github.com/spf13/viper"
  "fmt"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +  
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(  
  rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {  
  b := make([]byte, length)
  for i := range b {
    b[i] = charset[seededRand.Intn(len(charset))]
  }
  return string(b)
}

func String(length int) string {  
  return StringWithCharset(length, charset)
}

var bucket_counter = 1
var prefix = viper.GetString("fixtures.bucket_prefix")

func GetPrefix() string {

  return prefix
}

func GetBucketName() string {

  prefix := GetPrefix()
  random := String(6) 
  num := bucket_counter

  name := fmt.Sprintf("%s-%s-%d", prefix, random, num)

  return name
}

func Contains(slice []string, item string) bool {
    set := make(map[string]struct{}, len(slice))
    for _, s := range slice {
        set[s] = struct{}{}
    }

    _, ok := set[item] 
    return ok
}
