test:
	go test -race -v ./... -bench=. && \
	(cd _examples/simple-sqlite-example/ && go get -d && go run -race main.go)

