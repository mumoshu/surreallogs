#!/usr/bin/env bash

rm logs/myapp/app1.log; go run ./cmd/exampleapp -output logs/myapp/app1.log
