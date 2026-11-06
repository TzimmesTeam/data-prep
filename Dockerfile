FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /data-prep ./cmd/prep

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /data-prep /app/data-prep
COPY config/local.yml /app/config/local.yml
EXPOSE 8080
CMD ["/app/data-prep", "-config", "/app/config/local.yml"]
