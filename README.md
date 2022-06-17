[![Gem Version](https://badge.fury.io/rb/redis-cluster-client.svg)](https://badge.fury.io/rb/redis-cluster-client)
![Test status](https://github.com/redis-rb/redis-cluster-client/workflows/Test/badge.svg?branch=master)

Redis Cluster Client
===============================================================================
This library is a client for [Redis cluster](https://redis.io/docs/reference/cluster-spec/).
It depends on [redis-client](https://github.com/redis-rb/redis-client).
So it would be better to read `redis-client` documents first.

## Installation
```ruby
gem 'redis-cluster-client'
```

## Initialization
| key | type | default | description |
| --- | --- | --- | --- |
| `:nodes` | String or Array<String, Hash> | `['redis://127.0.0.1:6379']` | node addresses for startup connection |
| `:replica` | Boolean | `false` | `true` if client should use scale read feature |
| `:fixed_hostname` | String | `nil` | required if client should connect to single endpoint with SSL |

Also, [the other generic options](https://github.com/redis-rb/redis-client#configuration) can be passed.

```ruby
# The following examples are Docker containers on localhost.
# The client first attempts to connect to redis://127.0.0.1:6379 internally.

# To connect to primary nodes only
RedisClient.cluster.new_client
#=> #<RedisClient::Cluster 172.20.0.2:6379, 172.20.0.6:6379, 172.20.0.7:6379>

# To connect to all nodes to use scale reading feature
RedisClient.cluster(replica: true).new_client
#=> #<RedisClient::Cluster 172.20.0.2:6379, 172.20.0.3:6379, 172.20.0.4:6379, 172.20.0.5:6379, 172.20.0.6:6379, 172.20.0.7:6379>

# With generic options for redis-client
RedisClient.cluster(timeout: 3.0).new_client
```

```ruby
# To connect with subset nodes for startup
RedisClient.cluster(nodes: %w[redis://node1:6379 redis://node2:6379]).new_client
```

```ruby
# To connect with single endpoint
RedisClient.cluster(nodes: 'redis://endpoint.example.com:6379').new_client
```

```ruby
# To connect with single endpoint and SSL
RedisClient.cluster(nodes: 'rediss://endpoint.example.com:6379', fixed_hostname: 'endpoint.example.com').new_client
```

## Interfaces
The following methods are able to be used like `redis-client`.
* `#call`
* `#call_once`
* `#blocking_call`
* `#scan`
* `#sscan`
* `#hscan`
* `#zscan`
* `#pipelined`
* `#pubsub`
* `#close`

The other methods are not implemented because the client cannot operate with cluster mode.
`#pipelined` method splits and sends commands to each node and aggregates replies.

## Multiple keys and CROSSSLOT error
A part of commands can be passed multiple keys. But it has a constraint the keys are in the same hash slot.
The following error occurs because keys must be in the same hash slot and not just the same node.

```ruby
cli = RedisClient.cluster.new_client

cli.call('MGET', 'key1', 'key2', 'key3')
#=> CROSSSLOT Keys in request don't hash to the same slot (RedisClient::CommandError)

cli.call('CLUSTER', 'KEYSLOT', 'key1')
#=> 9189

cli.call('CLUSTER', 'KEYSLOT', 'key2')
#=> 4998

cli.call('CLUSTER', 'KEYSLOT', 'key3')
#=> 935
```

## Connection pooling
TODO

## Connection drivers
TODO
