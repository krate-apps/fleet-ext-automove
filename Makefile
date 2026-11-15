.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/automove-worker ./cmd/automove-worker

pack:
	bash scripts/pack.sh dist
