# Builds a minimal image containing only the solostratum program.
#
#   docker build -t solostratum .
#   docker run -d --name solostratum --restart unless-stopped \
#       -p 3333:3333 -p 3334:3334 -v "$PWD/data:/data" solostratum
#
# Put your solostratum.conf into the mounted data folder. Found blocks are
# saved to data/blocks and the mining history to data/stats.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /solostratum .

FROM scratch
# Root certificates, for nodes reached through an https:// address.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /solostratum /solostratum
VOLUME /data
EXPOSE 3333 3334
# Healthy means: running, and holding current work from the node.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD ["/solostratum", "-config", "/data/solostratum.conf", "-healthcheck"]
ENTRYPOINT ["/solostratum", "-config", "/data/solostratum.conf"]
