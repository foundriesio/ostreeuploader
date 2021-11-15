.PHONY: dir

GO ?= go
GOBUILDFLAGS ?=
bd = bin
push_exe = fiopush
check_exe = fiocheck
sync_exe = fiosync
linter := $(shell which golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)

all: $(push_exe) $(check_exe) $(sync_exe)

$(bd):
	@mkdir -p $@

$(push_exe): $(bd) cmd/fiopush/main.go
	$(GO) build $(GOBUILDFLAGS) -o $(bd)/$@ cmd/fiopush/main.go

$(check_exe): $(bd) cmd/fiocheck/main.go
	$(GO) build $(GOBUILDFLAGS) -o $(bd)/$@ cmd/fiocheck/main.go

$(sync_exe): $(bd) cmd/fiosync/main.go
	$(GO) build $(GOBUILDFLAGS) -o $(bd)/$@ cmd/fiosync/main.go

clean:
	@rm -r $(bd)

format:
	@gofmt -l -w ./

check:
	@test -z $(shell gofmt -l ./) || echo "[WARN] Fix formatting issues with 'make format'"
	$(linter) run
