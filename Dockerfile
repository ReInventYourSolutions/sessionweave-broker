# syntax=docker/dockerfile:1
# Build context: the root of this repository.
# PYTHON_IMAGE lets a build pin the base by digest (the distributed e2e does).
ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE} AS base
WORKDIR /app

FROM base AS build
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
COPY ci/sdk-prerelease.txt ./ci/
# sdk-prerelease: the SDK comes from a commit-pinned git reference until it is on
# PyPI (git is needed for it); then drop the COPY above, the git install and
# `-r ci/sdk-prerelease.txt`. One `pip wheel` command, so the pinned reference
# is used instead of a PyPI lookup of the name.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
# While the module repositories are private, an optional BuildKit secret
# `git_token` (read access) lets pip clone the pinned SDK; the credential is
# removed in the same layer and never reaches the runtime stage.
RUN --mount=type=secret,id=git_token \
    if [ -s /run/secrets/git_token ]; then \
      git config --global url."https://x-access-token:$(cat /run/secrets/git_token)@github.com/ReInventYourSolutions/".insteadOf "https://github.com/ReInventYourSolutions/"; \
    fi; \
    pip install --no-cache-dir --upgrade pip \
    && pip wheel -w /wheels -r ci/sdk-prerelease.txt .; \
    rc=$?; rm -f /root/.gitconfig; exit $rc

FROM base AS runtime
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels sessionweave-broker && rm -rf /wheels
ENV SW_HOST=0.0.0.0 \
    SW_PORT=8765
EXPOSE 8765
USER nobody
ENTRYPOINT ["sessionweave", "--host", "0.0.0.0", "--port", "8765"]
