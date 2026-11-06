# Use an official Go runtime as a parent image
FROM golang:1.27-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY ./HadesScheduler/HadesOperator/go.mod ./HadesScheduler/HadesOperator/go.mod
COPY ./HadesScheduler/HadesOperator/go.sum ./HadesScheduler/HadesOperator/go.sum
COPY ./HadesScheduler/go.mod ./HadesScheduler/go.mod
COPY ./HadesScheduler/go.sum ./HadesScheduler/go.sum
COPY ./shared/go.mod ./shared/go.mod
COPY ./shared/go.sum ./shared/go.sum
RUN cd HadesScheduler/HadesOperator && go mod download

# Copy the Go application source code into the container
COPY ./HadesScheduler/HadesOperator ./HadesScheduler/HadesOperator
COPY ./HadesScheduler ./HadesScheduler
COPY ./shared ./shared

# Build the Go application
WORKDIR /app/HadesScheduler/HadesOperator
RUN CGO_ENABLED=0 GOOS=linux go build -o manager ./cmd/main.go

# Start a new stage for the minimal runtime container
FROM gcr.io/distroless/static-debian12

# Set the working directory inside the minimal runtime container
WORKDIR /app

# Copy the built binary from the builder container into the minimal runtime container
COPY --from=builder /app/HadesScheduler/HadesOperator/manager /app/manager

# Run your Go application
USER 65532:65532
ENTRYPOINT ["/app/manager"]
