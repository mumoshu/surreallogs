#! /usr/bin/env bash

rm /tmp/surreallogs.buffer.pos /tmp/surreallogs.buffer

go build ./cmd/collector && ./collector
