# dueue

## Introduction

This is a super simple and not very feature-complete durable message queue (dueue).

## Getting started

```dockerfile
FROM tombailey256/dueue:0.0.0

ENV DURABILITY_ENGINE=postgres
ENV POSTGRES_USER=user
ENV POSTGRES_PASSWORD=password
ENV POSTGRES_DB=database
ENV POSTGRES_PORT=5432
ENV HTTP_PORT=8080
```

Note that currently, dueue does NOT support horizontal scaling. If you run dueue more than once, even with the same
durability engine, you will get unexpected results.

## HTTP API

### Publish a message

```shell
curl -XPOST localhost:8080/queues/my_queue/messages -d { "value": "example", "expiry": 1450747740 }
# 200 OK
# {
# 	"expiry": 1450747740,
# 	"id": "94d93556-3126-4aaa-8ddf-f30ee9daf27f",
# 	"value": "example"
# }
```

### Receive a message (if one is available)

```shell
curl -XGET localhost:8080/queues/my_queue/messages?subscriberId=my_subscriber
# 200 OK
# [
# 	{
# 		"expiry": 1450747740,
# 		"id": "94d93556-3126-4aaa-8ddf-f30ee9daf27f",
# 		"value": "example"
# 	}
# ]
```

### Acknowledge a message after it has been processed

```shell
curl -XDELETE localhost/dueue/myQueue/94d93556-3126-4aaa-8ddf-f30ee9daf27f
# 204 No Content
```

## Durability

Dueue is able to survive crashes, node failure, etc by storing messages using a durability engine.

The following durability engines are supported:

### In-memory

Stores messages in-memory. There is no durability if dueue is restarted or crashes. Configured with the following environment variables:

```sh
export DURABILITY_ENGINE="memory"
```

### Postgres

Stores messages using a [Postgres](https://www.postgresql.org/).

```sh
export DURABILITY_ENGINE="postgres"
export POSTGRES_USER="user"
export POSTGRES_PASSWORD="password"
export POSTGRES_DB="database"
export POSTGRES_PORT="5432"
```

Note, if the durability engine is unavailable some dueue operations will fail.

## Health check

Dueue has a built-in health check endpoint (`/health`) to confirm that it is working correctly. At the moment, it does
NOT confirm that the durability engine is working correctly.

## Future work

1. Better health checking
2. Metrics/observability?
