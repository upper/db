reset-db:
	make -C postgresql/_dumps all && \
	make -C mysql/_dumps all && \
	make -C mongo/_dumps all

test: reset-db
	go test ./... -v
