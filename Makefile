reset-db:
	$(MAKE) reset-db -C postgresql && \
	$(MAKE) reset-db -C mysql && \
	$(MAKE) reset-db -C ql && \
	$(MAKE) reset-db -C sqlite && \
	$(MAKE) reset-db -C mongo

test: reset-db
	go test ./... -v
