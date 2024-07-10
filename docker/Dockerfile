FROM python:3.10.12-slim-bookworm as builder

RUN pip install hatch
COPY . /source
WORKDIR /source
RUN hatch build

FROM python:3.10.12-slim-bookworm

COPY --from=builder /source/dist/*.whl /tmp/

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir $(echo /tmp/*.whl)[all]

ENTRYPOINT ["tini", "--"]
