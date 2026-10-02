VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
PLATFORMS := linux/amd64 linux/arm64 linux/arm darwin/amd64 darwin/arm64 windows/amd64

.PHONY: build test e2e e2e-clean release docker clean

# Build the program for this computer.
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o solostratum .

# Fast tests; no Docker needed.
test:
	go vet ./...
	go vet -tags e2e ./e2e/
	go test -race ./...

# End-to-end tests against real Bitcoin Core nodes in Docker.
# Choose the versions with: make e2e E2E_CORE_IMAGES="bitcoin/bitcoin:31"
e2e:
	go test -tags e2e -count=1 -timeout 30m ./e2e/

# Remove containers left behind by an interrupted e2e run.
e2e-clean:
	-docker ps -aq --filter label=solostratum-e2e | xargs -r docker rm -f -v

# Build release archives for every supported platform into dist/.
release:
	@rm -rf dist && mkdir -p dist
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; ext=; [ $$os = windows ] && ext=.exe; \
		dir=dist/solostratum-$(VERSION)-$$os-$$arch; mkdir -p $$dir; \
		echo "building $$p"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -ldflags "$(LDFLAGS)" -o $$dir/solostratum$$ext . || exit 1; \
		cp internal/config/solostratum.conf.example README.md LICENSE $$dir/; \
		if [ $$os = windows ]; then (cd dist && zip -qr $$(basename $$dir).zip $$(basename $$dir)); \
		else tar -C dist -czf $$dir.tar.gz $$(basename $$dir); fi; \
		rm -rf $$dir; \
	done
	@ls -1 dist

docker:
	docker build --build-arg VERSION=$(VERSION) -t solostratum:$(VERSION) -t solostratum:latest .

clean:
	rm -rf solostratum solostratum.exe dist
