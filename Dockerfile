# PYTHON_IMAGE lets a build pin the base by digest (the distributed e2e does).
ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE} AS base
WORKDIR /app

FROM base AS build
COPY pyproject.toml ./
COPY README.md ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip \
    && pip wheel --no-deps -w /wheels . \
    && pip wheel -w /wheels .

FROM base AS runtime
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
ENV SW_HOST=0.0.0.0 \
    SW_PORT=8765
EXPOSE 8765
USER nobody
ENTRYPOINT ["sessionweave", "--host", "0.0.0.0", "--port", "8765"]
