.PHONY: build vet smoke clean tidy

BIN := bin/trafficgen

build:
	@mkdir -p bin
	go build -o $(BIN) ./cmd/trafficgen

vet:
	go vet ./...

tidy:
	go mod tidy

smoke: build
	$(BIN) full --config scenarios/smoke.yaml

clean:
	rm -rf bin out/smoke
