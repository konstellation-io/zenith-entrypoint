
protoc -I=proto --go_out=./zenithproto --go_opt=paths=source_relative --go-grpc_out=./zenithproto --go-grpc_opt=paths=source_relative proto/public_input.proto

