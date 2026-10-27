FROM aiagent-gateway-systemd-test:bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends e2fsprogs xfsprogs \
    && rm -rf /var/lib/apt/lists/*
