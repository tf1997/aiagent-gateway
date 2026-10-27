ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE}
ENV container=docker
RUN apt-get update \
    && apt-get install -y --no-install-recommends systemd dbus python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --system --gid 987 gateway-probe \
    && useradd --system --uid 987 --gid 987 --home-dir /nonexistent --shell /usr/sbin/nologin gateway-probe
STOPSIGNAL SIGRTMIN+3
COPY boot.sh /usr/local/bin/boot
RUN chmod 0755 /usr/local/bin/boot
CMD ["/usr/local/bin/boot"]
