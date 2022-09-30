# https://hub.docker.com/_/debian
FROM debian:11 as builder

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential wget unzip ca-certificates

# https://github.com/RedisLabs/redis-cluster-proxy
ARG TAG=1.0-beta2
WORKDIR /tmp
RUN wget -O redis-cluster-proxy.zip "https://github.com/RedisLabs/redis-cluster-proxy/archive/refs/tags/${TAG}.zip"
RUN unzip redis-cluster-proxy.zip
WORKDIR /tmp/redis-cluster-proxy-${TAG}
RUN make install REDIS_CLUSTER_PROXY_LDFLAGS=-zmuldefs

# https://github.com/GoogleContainerTools/distroless
FROM gcr.io/distroless/cc-debian11:debug
COPY --from=builder /usr/local/bin/redis-cluster-proxy /usr/local/bin/redis-cluster-proxy
ENTRYPOINT ["/usr/local/bin/redis-cluster-proxy"]
