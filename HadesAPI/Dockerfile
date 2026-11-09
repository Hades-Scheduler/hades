# syntax=docker/dockerfile:1

# Stage 1: build the dashboard SPA. Its output is embedded into the Go binary.
FROM node:22-alpine AS ui
WORKDIR /ui
COPY ./HadesAPI/web/package.json ./HadesAPI/web/package-lock.json ./
RUN npm ci
COPY ./HadesAPI/web ./
RUN npm run build

# Stage 2: build the Go binary, embedding the built SPA.
FROM golang:1.27-alpine AS builder
WORKDIR /app

# Copy go.mod and go.sum files
COPY ./HadesAPI/go.mod ./HadesAPI/go.sum ./HadesAPI/
COPY ./shared/go.mod ./shared/go.sum ./shared/
RUN cd HadesAPI && go mod download

# Copy the Go application source code into the container
COPY ./HadesAPI ./HadesAPI
COPY ./shared ./shared

# Overlay the freshly built SPA so //go:embed picks up the real assets instead
# of the committed placeholder.
COPY --from=ui /ui/dist ./HadesAPI/web/dist

# Build the Go application
WORKDIR /app/HadesAPI
RUN CGO_ENABLED=0 go build -o hadesCI-api .

# Stage 3: minimal runtime container
FROM gcr.io/distroless/static-debian12

WORKDIR /app

# Copy the built binary from the builder container into the minimal runtime container
COPY --from=builder /app/HadesAPI/hadesCI-api .

# Run as a non-root user
USER 65532:65532

# Run your Go application
CMD ["/app/hadesCI-api"]
