# Step 1: Build the Go binary
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
# Build the combined server (or specific web server)
RUN go build -o main ./web/main.go 

# Step 2: Final lightweight image
FROM alpine:latest
WORKDIR /root/
# Copy the binary from the builder
COPY --from=builder /app/main .
# IMPORTANT: Copy your HTML/static files for HTMX to work
COPY --from=builder /app/web/index.html ./web/
EXPOSE 8080
CMD ["./main"]