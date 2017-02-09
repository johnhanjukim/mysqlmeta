#!/bin/bash
export GOROOT=/usr/local/go
export GOPATH=$PWD
go run test.go $1
