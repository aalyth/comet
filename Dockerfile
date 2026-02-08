FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BUILD_TARGET=./cmd/comet
RUN CGO_ENABLED=0 go build -o /binary ${BUILD_TARGET}

FROM alpine:latest

COPY --from=builder /binary /binary
EXPOSE 6174
ENTRYPOINT ["/binary"]
