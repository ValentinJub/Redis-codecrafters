[![progress-banner](https://backend.codecrafters.io/progress/redis/9541e44c-1cee-46fe-89d7-1a57b455fe2d)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# Implemented features

- Can launch the server as a master or a replica
- The master replicates its data to connected replicas
- The master can use an rdb file to load data in memory from disk

# Implemented commands

- `DISCARD`
- `ECHO`
- `EXEC`
- `GET`
- `INFO`
- `INCR`
- `KEYS`
- `MULTI`
- `PING`
- `PSYNC`
- `REPLCONF`
- `SET`
- `TYPE`
- `WAIT`
- `XADD`
- `XRANGE`
- `XREAD`


# Todo

## Features

## Commands

- `DEL`
- `EXISTS`
- `COPY`

## Design

- Refactor ReqHandler so that it can send the responses to the client, currently it processes the request and returns the response a slice of byte
that the server then sends to the client.
It can use the SendTo method of its server to send the response to the client.
- Improve ReqHandler to properly handle multiple request in the same buffer
- Improve request parsing to handle multiple requests in the same buffer
