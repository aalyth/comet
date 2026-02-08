FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /comet ./cmd/comet

FROM alpine:latest

COPY --from=builder /comet /comet
EXPOSE 6174
ENTRYPOINT ["/comet"]
