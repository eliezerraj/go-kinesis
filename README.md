# go-kinesis

POC for technical purposes

This POC create a dummy message and send to a kinesis stream (as a producer) and consumer messages from kinesis stream as a consumer

## producer

Create messages using templates in folder ./msg (using func data.file) or send a struct (using a fun data.mock) and send it to a kinesis stream using a partition-key set in .env file or via flag, see example below


    go run . -k key1

## consumer

consumer messages from kinesis stream using a shard iterator type as TRIM, AT_SEQUENCE_NUMBER or LATEST