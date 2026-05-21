proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		proto/state.proto
	mkdir -p gen/statepb
	mv proto/state.pb.go gen/statepb/
