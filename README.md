
 ## S3 compatibility tests

This is a set of completely unofficial Amazon AWS S3 compatibility
tests, that will hopefully be useful to people implementing software
that exposes an S3-like API.

The tests only cover the REST interface.

### Setup

The tests use the [GO amazon SDK](). The tests use the testing built in Go Package and an assertion toolkit testify.To get started, ensure you have the Golang Environment installed software installed; e.g. on Debian/Ubuntu::

Ubuntu

	sudo apt-get install golang 

Fedora

	dnf install golang-gopkg-yaml-devel-v2 \
	golang-github-aws-aws-sdk-go-devel \
	golang-github-stretchr-testify-devel


### Running the Tests

Clone the repository

	git clone https://github.com/nanjekyejoannah/go_s3tests
	cd go_s3tests

You will need to create a configuration file with the location of the service and credentials. You can edit the config.toml.sample file available in the project. Make sure to save it as config.toml. You can also decide to make the config file a yaml or json. Just give it config.yaml or config.json for yaml and json respectively. The tests connect to the Ceph RGW ,therefore you shoud have started your RGW and use the credentials you get. The config file looks like this:

	[DEFAULT]

	host = "s3.amazonaws.com"
	port = "8080"
	is_secure = "yes"

	[fixtures]

	bucket_prefix = "jnans"

	[s3main]

	access_key = "0555b35654ad1656d804"
	access_secret = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
	bucket = "bucket1"
	region = "mexico"
	endpoint = "http://localhost:8000/"
	display_name = ""
	email = "someone@gmail.com"

	[s3alt]

	access_key = "0555b35654ad1656d804"
	access_secret = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
	bucket = "bucket1"
	region = "mexico"
	endpoint = "http://localhost:8000/"
	display_name = ""
	email = "someone@gmail.com"

You need to set your GoPath .Details on setting up Ho environments can be found [here](https://golang.org/doc/install)
	
	export GOPATH=$HOME/go

### Installing dependencies

	go get -u github.com/aws/aws-sdk-go
	go get github.com/spf13/viper
	go get github.com/stretchr/testify/require

### To run the tests
	
	cd s3tests
	go test        			 # to run all tests
	go test <filename>       # to run specific test file
