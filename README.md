[![Gem Version](https://badge.fury.io/rb/redis-cluster-client.svg)](https://badge.fury.io/rb/redis-cluster-client)
![Test status](https://github.com/redis-rb/redis-cluster-client/workflows/Test/badge.svg?branch=master)

Redis Cluster Client
===============================================================================

TODO

```ruby
cli = RedisClient.cluster(nodes: %w[redis://127.0.0.1:7000]).new_client

cli.call('PING')
#=> PONG
```
