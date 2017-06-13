=======================
 S3 compatibility tests
========================

This is a set of completely unofficial Amazon AWS S3 compatibility
tests, that will hopefully be useful to people implementing software
that exposes an S3-like API.

The tests only cover the REST interface.

### Setup

The tests use the [GO amazon SDK](). The tests use the testing built in Go Package and an assertion toolkit testify.To get started, ensure you have the Golang Environment installed software installed; e.g. on Debian/Ubuntu::

Ubuntu

	sudo apt-get install golang 

Fedora

	sudo dnf install golang

### Running the Tests

Clone the repository

	git clone https://github.com/nanjekyejoannah/go_s3tests

You will need to create a configuration file with the location of the service and credentials. You can edit the config.yaml.sample file available in the project. Make sure to save it as config.yaml. The tests connect to the Ceph RGW ,therefore you shoud have started your RGW and use the credentials given on start. It looks like this:


	appname: go_s3test

	RGW:
	    key:     	0555b35654ad1656d804
	    secret:     h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
	    bucket: 	bucket1
	    region:     mexico
	    endpoint:	http://localhost:8000/

	default:
		key:     	0555b35654ad1656d804
	    secret:     h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
	    bucket: 	bucket1
	    region:     mexico
    	endpoint:	http://localhost:8000/

You need to set your GoPath
	
	export GOPATH=$HOME/go

Get the Amazon Go SDK

	go get -u github.com/aws/aws-sdk-go

To run the tests
	
	cd s3tests
	go test        			 # to run all tests
	go test <filename>       # to run specific test file

