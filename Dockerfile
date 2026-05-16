FROM python:3.12-slim AS base
WORKDIR /app

FROM base AS build
# git is required to resolve VCS dependencies (sessionweave-client is
# pulled from git+https). Installed only in the build stage; the runtime
# image does not need it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml ./
COPY README.md ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip \
    && pip wheel --no-deps -w /wheels . \
    && pip wheel -w /wheels .

FROM base AS runtime
COPY --from=build /wheels /wheels
# --no-deps: every transitive wheel is already in /wheels (built in
# the prior stage), so pip should not try to resolve metadata —
# especially the sessionweave-client @ git+ direct URL, which would
# require git in the runtime image.
RUN pip install --no-cache-dir --no-deps /wheels/*.whl && rm -rf /wheels
ENV SW_HOST=0.0.0.0 \
    SW_PORT=8765
EXPOSE 8765
USER nobody
ENTRYPOINT ["sessionweave", "--host", "0.0.0.0", "--port", "8765"]
