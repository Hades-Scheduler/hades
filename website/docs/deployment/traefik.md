---
sidebar_position: 2
---

# Traefik Deployment

Hades ships with a ready-to-use Traefik configuration that exposes the API over **HTTPS with automatic TLS** via Let's Encrypt. This is the recommended way to deploy Hades on a VM or bare-metal server.

## Overview

The Traefik setup uses Docker Compose and consists of two compose files that are layered together:

| File | Purpose |
|---|---|
| `compose.yml` | Core Hades services (API, Scheduler, NATS) |
| `docker-compose.deploy.yml` | Adds Traefik reverse proxy with TLS and removes the host port bindings of every service except Traefik |

Traefik handles:
- HTTP → HTTPS redirect (port 80 → 443)
- Automatic TLS certificate issuance and renewal via [Let's Encrypt](https://letsencrypt.org) (ACME HTTP-01 challenge)
- Reverse proxying to the `hadesAPI` service

## Prerequisites

- A **public server** (VM, VPS, dedicated host) with Docker and Docker Compose installed
- A **domain name** pointing to the server's public IP (e.g., `hades.example.com`)
- Ports **80** and **443** open in the server's firewall

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Hades-Scheduler/hades.git
cd hades
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and set the following values:

```env
# Your email address - Let's Encrypt will send certificate expiry notices here
LETSENCRYPT_EMAIL=you@example.com

# The public domain name for the Hades API
HADES_API_HOST=hades.example.com

# Required: Basic Auth key for POST /build (user "hades"); compose refuses to start without it
AUTH_KEY=<output of: openssl rand -hex 32>
```

### 3. Create the Traefik ACME Storage File

Traefik stores issued certificates in a local JSON file. Create it with the correct permissions:

```bash
touch traefik/acme.json
chmod 600 traefik/acme.json
```

:::warning
The `acme.json` file must have mode `0600`. If permissions are too open, Traefik will refuse to start.
:::

### 4. Deploy

Start all services using both compose files:

```bash
docker compose -f compose.yml -f docker-compose.deploy.yml up -d
```

This starts:
- **Traefik** on ports 80 and 443 - handles TLS termination and routing
- **hadesAPI** - reachable only through Traefik (no host port binding)
- **hadesScheduler** - connected to the Docker socket
- **nats** - internal only (no host port binding; it has no authentication, so never publish it)
- **hadesLogManager** and **jaeger** - internal only, not routed through Traefik

The log manager has no authentication of its own and is not exposed. The dashboard reads logs through its authenticated proxy.

## How It Works

### Traefik Configuration (`traefik/traefik.yml`)

```yaml
log:
  level: INFO

api:
  dashboard: true

entryPoints:
  web:
    address: ":80"
    http:
      redirections:
        entryPoint:
          to: websecure
          scheme: https

  websecure:
    address: ":443"

providers:
  docker:
    exposedByDefault: false

certificatesResolvers:
  letsencrypt:
    acme:
      storage: /etc/traefik/acme.json
      httpChallenge:
        entryPoint: web
```

Key points:
- `exposedByDefault: false` - only services with the `traefik.enable=true` label are exposed.
- The `web` entrypoint automatically redirects all HTTP traffic to HTTPS.
- The `letsencrypt` resolver uses the **HTTP-01 challenge** over port 80 to verify domain ownership.

### Docker Labels on `hadesAPI`

The API service in `docker-compose.deploy.yml` uses Traefik Docker labels to configure routing:

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.hadesapi.rule=Host(`${HADES_API_HOST}`)"
  - "traefik.http.routers.hadesapi.entrypoints=web,websecure"
  - "traefik.http.routers.hadesapi.tls.certresolver=letsencrypt"
  - "traefik.http.services.hadesapi.loadbalancer.server.port=8080"
```

| Label | Effect |
|---|---|
| `traefik.enable=true` | Opt this container into Traefik routing |
| `rule=Host(...)` | Route requests matching the configured hostname to this service |
| `entrypoints=web,websecure` | Accept traffic on both HTTP and HTTPS entrypoints |
| `tls.certresolver=letsencrypt` | Use Let's Encrypt to issue and renew the TLS certificate |
| `loadbalancer.server.port=8080` | Forward traffic to the container's internal port 8080 |

## Verifying the Deployment

### Check Container Status

```bash
docker compose -f compose.yml -f docker-compose.deploy.yml ps
```

All services should show `running`. Traefik may take 30-60 seconds on first start to issue a certificate.

### Test HTTPS

```bash
curl https://hades.example.com/ping
```

Or submit a test job:

```bash
AUTH_KEY=<the AUTH_KEY from .env>
curl -X POST https://hades.example.com/build \
  -u "hades:$AUTH_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Traefik Test",
    "steps": [
      {
        "id": 1,
        "name": "Echo",
        "image": "alpine:latest",
        "script": "echo Hello over HTTPS!"
      }
    ]
  }'
```

### Check Traefik Logs

```bash
docker logs traefik -f
```

Look for lines like:

```text
msg="Configuration loaded from file: /etc/traefik/traefik.yml"
msg="Starting server" entryPointName=web
msg="Starting server" entryPointName=websecure
```

## Updating Hades

To pull the latest images and restart the stack:

```bash
docker compose -f compose.yml -f docker-compose.deploy.yml pull
docker compose -f compose.yml -f docker-compose.deploy.yml up -d
```

## Stopping the Stack

```bash
docker compose -f compose.yml -f docker-compose.deploy.yml down
```

The `acme.json` file is preserved on disk, so certificates are retained between restarts.

## Ansible Automation

For fully automated VM provisioning and deployment, Hades includes Ansible playbooks. See the `ansible/hades/README.md` in the repository for details.

