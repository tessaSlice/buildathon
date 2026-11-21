# Build the image the scorer builds: the Dockerfile at the repository root,
# for linux/amd64.
IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64

.PHONY: build score score-dev

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

# Practice-sized clock. This is the number the scorer publishes.
# Point PRACTICE at a generated ~20M directory. There is no public answer.
PRACTICE ?= /tmp/shopping-20m
score:
	./scripts/bench_score.sh $(PRACTICE)

# Correctness gate only. The scorer runs this before the timed practice set
# and does not publish it as the score.
score-dev:
	./scripts/bench_score.sh /tmp/shopping-dev $(CURDIR)/dev-answer
