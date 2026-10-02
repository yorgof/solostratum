# cpuminer (pooler's "minerd"), an independent, widely used Stratum client.
# It is used as a second opinion: it was written by other people long before
# this project and knows nothing about it.
FROM debian:trixie-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        autoconf automake build-essential ca-certificates git \
        libcurl4-openssl-dev libjansson-dev \
    && rm -rf /var/lib/apt/lists/*
ARG CPUMINER_REF=v2.5.1
RUN git clone --depth 1 --branch "${CPUMINER_REF}" https://github.com/pooler/cpuminer.git /cpuminer
WORKDIR /cpuminer
RUN ./autogen.sh && ./configure CFLAGS="-O2 -std=gnu17" && make -j"$(nproc)"

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libcurl4t64 libjansson4 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /cpuminer/minerd /usr/local/bin/minerd
ENTRYPOINT ["minerd"]
