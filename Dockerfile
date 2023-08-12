# Use an official Go runtime as the base image
ARG GO_VERSION=1.20
FROM golang:${GO_VERSION}

RUN apt-get update && apt-get install -y jq

# ------------------------------------
WORKDIR /xtkt
# --
COPY cli/cmd cmd
COPY cli/lib lib
COPY cli/sources sources
COPY cli/util util
COPY cli/go.mod .
COPY cli/go.sum .
COPY cli/main.go .
RUN go mod vendor
RUN go build .
# --
RUN mv ./xtkt /usr/local/bin/
# ------------------------------------
WORKDIR /xtkt_api
COPY api/main.go .
# ------------------------------------
WORKDIR /
# Expose the port the API will run on
EXPOSE 8000
CMD ["sh", "-c", "go run /xtkt_api/main.go"]