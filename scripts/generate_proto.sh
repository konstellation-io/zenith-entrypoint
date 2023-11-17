#!/usr/bin/bash

protoc -I=proto --go_out=./internal/proto/krepb --go_opt=paths=source_relative \
    --go-grpc_out=./internal/proto/krepb --go-grpc_opt=paths=source_relative \
    ./proto/kre_nats_msg.proto

protoc -I=proto --go_out=./internal/proto/publicpb --go_opt=paths=source_relative \
    --go-grpc_out=./internal/proto/publicpb --go-grpc_opt=paths=source_relative \
    ./proto/publicpb/public_input.proto
