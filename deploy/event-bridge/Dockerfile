FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY deploy/event-bridge ./deploy/event-bridge
RUN CGO_ENABLED=0 go build -o /event-bridge ./deploy/event-bridge

FROM alpine:3.20
COPY --from=builder /event-bridge /event-bridge
EXPOSE 8080
CMD ["/event-bridge"]
