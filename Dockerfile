# Stage 1: Build the application
FROM golang:1.22 AS base

WORKDIR /code

# Copy go.mod and go.sum to download dependencies
COPY src/go.mod src/go.sum src/
RUN cd src && go mod download

# Copy the source code
COPY . .

# Build the application

FROM base AS builder

RUN cd src && CGO_ENABLED=0 GOOS=linux go build -o /code/infra-auditlog-service

# Stage 2: Run unit tests
FROM builder AS ccs-app-code

# Run unit tests
RUN bin/run_test

# Stage 3: Final minimal image
FROM alpine:3.18

# Install ca-certificates for HTTPS (needed for AWS SDK)
RUN apk add --no-cache ca-certificates

# Set working directory
WORKDIR /code

# Copy the compiled binary from the builder stage
COPY --from=builder /code/infra-auditlog-service ./infra-auditlog-service
COPY bin/start ./start

# Expose port 8080
EXPOSE 8080

# Command to run the service
CMD ["/code/start"]
