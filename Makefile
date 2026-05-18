VERSION ?= 0.1.0
LDFLAGS = -s -w -X main.version=$(VERSION)

.PHONY: build build-all clean

build:
	go build -ldflags "$(LDFLAGS)" -o dist/bifrost-channel .

build-all:
	mkdir -p dist
	GOOS=linux   GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/bifrost-channel-linux-amd64 .
	GOOS=darwin  GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/bifrost-channel-darwin-amd64 .
	GOOS=darwin  GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/bifrost-channel-darwin-arm64 .
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/bifrost-channel-windows-amd64.exe .
	@ls -lh dist/

clean:
	rm -rf dist/
