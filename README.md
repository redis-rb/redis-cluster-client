[![Gem Version](https://badge.fury.io/rb/redis-cluster-client.svg)](https://badge.fury.io/rb/redis-cluster-client)

TODO
===============================================================================

```ruby
cli = RedisClient.cluster(nodes: %w[redis://127.0.0.1:7000], replica: false).new_client

cli.call('PING')
#=> PONG
```
