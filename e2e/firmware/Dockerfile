# Builds a small command-line miner out of the Bitaxe firmware's own Stratum
# job code (ESP-Miner, GPL-3.0), fetched from its public repository at build
# time. Nothing from the firmware is stored in this repository.
#
# The point: the header that gets hashed is assembled by the same C code
# that runs on a real Bitaxe, not by a re-implementation.
FROM debian:trixie-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates git libmbedtls-dev \
    && rm -rf /var/lib/apt/lists/*
ARG ESP_MINER_REF=v2.14.1
RUN git clone --depth 1 --branch "${ESP_MINER_REF}" https://github.com/bitaxeorg/ESP-Miner.git /esp-miner
COPY shim /shim
COPY fwminer.c /fwminer.c
# The firmware relies on ESP-IDF pulling in a few standard headers and on
# ESP-IDF's mbedTLS configuration; the flags below supply the same on Linux.
RUN cc -O2 -o /fwminer \
        -include stdlib.h -include stdbool.h -DMBEDTLS_ALLOW_PRIVATE_ACCESS \
        -I/shim -I/esp-miner/components/stratum/include \
        /fwminer.c \
        /esp-miner/components/stratum/mining.c \
        /esp-miner/components/stratum/utils.c \
        -lmbedcrypto -lm

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libmbedcrypto16 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /fwminer /usr/local/bin/fwminer
ENTRYPOINT ["fwminer"]
