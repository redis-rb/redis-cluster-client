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
| `:nodes` | String or Hash or Array<String, Hash> | `['redis://127.0.0.1:6379']` | node addresses for startup connection |
| `:replica` | Boolean | `false` | `true` if client should use scale read feature |
| `:fixed_hostname` | String | `nil` | required if client should connect to single endpoint with SSL |

Also, [the other generic options](https://github.com/redis-rb/redis-client#configuration) can be passed.
But `:url`, `:host`, `:port` and `:path` are ignored because they conflict with the `:nodes` option.

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
# To connect with a subset of nodes for startup
RedisClient.cluster(nodes: %w[redis://node1:6379 redis://node2:6379]).new_client
```

```ruby
# To connect with single endpoint
RedisClient.cluster(nodes: 'redis://endpoint.example.com:6379').new_client
```

```ruby
# To connect to single endpoint with SSL/TLS
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
A subset of commands can be passed multiple keys. But it has a constraint the keys are in the same hash slot.
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

Also, you can use the hash tag to bias keys to the same slot.

```ruby
cli.call('CLUSTER', 'KEYSLOT', '{key}1')
#=> 12539

cli.call('CLUSTER', 'KEYSLOT', '{key}2')
#=> 12539

cli.call('CLUSTER', 'KEYSLOT', '{key}3')
#=> 12539

cli.call('MGET', '{key}1', '{key}2', '{key}3')
#=> [nil, nil, nil]
```

## ACL
The cluster client internally calls [COMMAND](https://redis.io/commands/command/) and [CLUSTER NODES](https://redis.io/commands/cluster-nodes/) commands to operate correctly.
So please permit it like the followings.

```ruby
# The default user is administrator.
cli1 = RedisClient.cluster.new_client

# To create a user with permissions
# Typically, user settings are configured in the config file for the server beforehand.
cli1.call('ACL', 'SETUSER', 'foo', 'ON', '+COMMAND', '+CLUSTER|NODES', '+PING', '>mysecret')

# To initialize client with the user
cli2 = RedisClient.cluster(username: 'foo', password: 'mysecret').new_client

# The user can only call the PING command.
cli2.call('PING')
#=> "PONG"

cli2.call('GET', 'key1')
#=> NOPERM this user has no permissions to run the 'get' command (RedisClient::PermissionError)
```

Otherwise:

```ruby
RedisClient.cluster(username: 'foo', password: 'mysecret').new_client
#=> Redis client could not fetch cluster information: NOPERM this user has no permissions to run the 'cluster|nodes' command (RedisClient::Cluster::InitialSetupError)
```

## Connection pooling
You can use the internal connection pooling feature implemented by [redis-client](https://github.com/redis-rb/redis-client#usage) if needed.

```ruby
# example of docker on localhost
RedisClient.cluster.new_pool(timeout: 1.0, size: 2)
#=> #<RedisClient::Cluster 172.21.0.3:6379, 172.21.0.6:6379, 172.21.0.7:6379>
```

## Connection drivers
Please see [redis-client](https://github.com/redis-rb/redis-client#drivers).

## See also
* https://github.com/redis/redis-rb/issues/1070
* https://github.com/redis/redis/issues/8948
* https://github.com/antirez/redis-rb-cluster
