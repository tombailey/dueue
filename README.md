# dueue

## Introduction

This is a super simple and not very feature-complete durable queue (dueue).

## Getting started

```dockerfile
FROM tombailey256/dueue:0.2.0

ENV DURABILITY_ENGINE="postgres"
ENV POSTGRES_HOST="postgres"
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER="user"
ENV POSTGRES_PASSWORD="password"
ENV POSTGRES_DATABASE="database"

ENV HTTP_PORT = 8080
```

Note that currently, dueue does NOT support horizontal scaling. If you run dueue on more than one node, even with the same durability engine, you will get unexpected results.

## HTTP API

### Publish a message
```text
$> curl -XPOST localhost/dueue/myQueue -d { "message": "test" }
204 No Content
```

### Receive a message (if one is available)
```text
$> curl -XGET localhost/dueue/myQueue
200 OK
{
	"id": "ae41344a-888e-4608-8870-2a4e57b955d6",
	"message": "test"
}
```

### Acknowledge a message so it can be deleted after it has been processed
```text
$> curl -XDELETE localhost/dueue/myQueue/ae41344a-888e-4608-8870-2a4e57b955d6
204 No content
```


## Durability

Dueue is able to survive crashes, node failure, etc by storing messages using a durability engine.

The following durability engines are supported:

### In-memory

Stores messages in-memory. There is no durability if dueue is restarted or crashes. Configured with the following environment variables:

```sh
export DURABILITY_ENGINE="memory"
```

### Firestore

Stores messages using a [Firestore collection](https://firebase.google.com/docs/firestore/data-model). Configured with the following environment variables:

```sh
export DURABILITY_ENGINE="firestore"
export FIRESTORE_CREDENTIALS_FILE="/app/firebase-admin-sdk-service-account-credentials.json"
export FIRESTORE_COLLECTION="dueue"
```

### PostgreSQL

Stores messages using a table (named dueue). Configured with the following environment variables:

```sh
export DURABILITY_ENGINE="postgres"
export POSTGRES_HOST="postgres"
export POSTGRES_PORT=5432
export POSTGRES_USER="user"
export POSTGRES_PASSWORD="password"
export POSTGRES_DATABASE="database"
```

Note, if the durability engine is unavailable dueue operations (even receive) will fail.

## Health check

Dueue has a built-in health check endpoint (`/health`) to confirm that it is working correctly. At the moment, it does NOT confirm that the durability engine is working correctly.

## Future work

1. Better health checking
2. Metrics/observability?
