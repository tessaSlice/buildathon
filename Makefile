# Build the image the scorer builds: the Dockerfile at the repository root,
# for linux/amd64.
IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64

.PHONY: build
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .
