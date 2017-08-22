
# Test Go SDK on RGW

Clone the Repository

	git clone https://github.com/nanjekyejoannah/go_s3tests

Checkout RGWTEST branch

	git checkout RGWTEST

Change to project directory

	cd go_s3tests

Run vstart

	../src/vstart.sh  -n -l --rgw_num 1

Install golang

Ubuntu

	sudo apt-get install golang 

Fedora 

	sudo apt-get install golang 

Set Go path

	export GOPATH=$HOME/go

Install dependencies

	go get -d ./...

To list Buckets that you created using s3cmd on RGW or some other way.

	go run main.go

To create Buckets

	cd create
	go run main.go






