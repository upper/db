setup-test:
	$(MAKE) reset-db generate -C postgresql && \
	$(MAKE) reset-db generate -C mysql && \
	$(MAKE) reset-db generate -C ql && \
	$(MAKE) reset-db generate -C sqlite && \
	$(MAKE) reset-db generate -C mongo && \
	go get -d -t -v ./...

test: setup-test
	go test ./... -v
