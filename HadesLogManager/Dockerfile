# Use an official Go runtime as a parent image
FROM golang:1.27-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY ./HadesLogManager/go.mod ./HadesLogManager/go.sum ./HadesLogManager/
COPY ./shared/go.mod ./shared/go.sum ./shared/
RUN cd HadesLogManager && go mod download

# Copy the Go application source code into the container
COPY ./HadesLogManager ./HadesLogManager
COPY ./shared ./shared

# Build the Go application
WORKDIR /app/HadesLogManager
RUN CGO_ENABLED=0 go build -o hades-log-manager .

# Start a new stage for the minimal runtime container
FROM gcr.io/distroless/static-debian12

# Set the working directory inside the minimal runtime container
WORKDIR /app

# Copy the built binary from the builder container into the minimal runtime container
COPY --from=builder /app/HadesLogManager/hades-log-manager .

# Run as a non-root user
USER 65532:65532

# Run your Go application
CMD ["/app/hades-log-manager"]