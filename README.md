
# Test Go SDK on RGW

Clone the Repository

	git clone https://github.com/nanjekyejoannah/go_s3tests

Checkout list-buckets-rgw-test branch

	git checkout list-buckets-rgw-test

Change to project directory

	cd go_s3tests

Run vstart

	../src/vstart.sh  -n -l --rgw_num 1

Install golang

Ubuntu

	sudo apt-get install golang 

Fedora 

	sudo dnf install golang 

Install boto3 and botocore

Ubuntu & Fedora

	pip install boto3
	pip install botocore

Set Go path

	export GOPATH=$HOME/go

Install dependencies

	go get -d ./...

To create a Bucket that you can list using s3cmd or list-buckets-rgw.py in the RGW

        cd create
	go run main.go
        python list-buckets-rgw.py
        (output should be: Bucket List: ['bkt1'])

