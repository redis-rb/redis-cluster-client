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
| `:replica_affinity` | Symbol or String | `:random` | scale reading strategy, `:random` or `:latency` are valid |
| `:fixed_hostname` | String | `nil` | required if client should connect to single endpoint with SSL |

Also, [the other generic options](https://github.com/redis-rb/redis-client#configuration) can be passed.
But `:url`, `:host`, `:port` and `:path` are ignored because they conflict with the `:nodes` option.

```ruby
require 'redis_cluster_client'

# The following examples are Docker containers on localhost.
# The client first attempts to connect to redis://127.0.0.1:6379 internally.

# To connect to primary nodes only
RedisClient.cluster.new_client
#=> #<RedisClient::Cluster 172.20.0.2:6379, 172.20.0.6:6379, 172.20.0.7:6379>

# To connect to all nodes to use scale reading feature
RedisClient.cluster(replica: true).new_client
#=> #<RedisClient::Cluster 172.20.0.2:6379, 172.20.0.3:6379, 172.20.0.4:6379, 172.20.0.5:6379, 172.20.0.6:6379, 172.20.0.7:6379>

# To connect to all nodes to use scale reading feature prioritizing low-latency replicas
RedisClient.cluster(replica: true, replica_affinity: :latency).new_client
#=> #<RedisClient::Cluster 172.20.0.2:6379, 172.20.0.3:6379, 172.20.0.4:6379, 172.20.0.5:6379, 172.20.0.6:6379, 172.20.0.7:6379>

# With generic options for redis-client
RedisClient.cluster(timeout: 3.0).new_client
```

```ruby
# To connect with a subset of nodes for startup
RedisClient.cluster(nodes: %w[redis://node1:6379 redis://node2:6379]).new_client
```

```ruby
# To connect with a subset of auth-needed nodes for startup

# User name and password should be URI encoded and the same in every node.
username = 'myuser'
password = URI.encode_www_form_component('!&<123-abc>')

# with URL:
RedisClient.cluster(nodes: %W[redis://#{username}:#{password}@node1:6379 redis://#{username}:#{password}@node2:6379]).new_client

# with options:
RedisClient.cluster(nodes: %w[redis://node1:6379 redis://node2:6379], username: username, password: password).new_client
```

```ruby
# To connect to single endpoint
RedisClient.cluster(nodes: 'redis://endpoint.example.com:6379').new_client
```

```ruby
# To connect to single endpoint with SSL/TLS (such as Amazon ElastiCache for Redis)
RedisClient.cluster(nodes: 'rediss://endpoint.example.com:6379').new_client
```

```ruby
# To connect to NAT-ted endpoint with SSL/TLS (such as Microsoft Azure Cache for Redis)
RedisClient.cluster(nodes: 'rediss://endpoint.example.com:6379', fixed_hostname: 'endpoint.example.com').new_client
```

## Interfaces
The following methods are able to be used like `redis-client`.
* `#call`
* `#call_v`
* `#call_once`
* `#call_once_v`
* `#blocking_call`
* `#blocking_call_v`
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

## Development
Please make sure the following tools are installed on your machine.

| Tool | Version | URL |
| --- | --- | --- |
| Docker | latest stable | https://docs.docker.com/engine/install/ |
| Docker Compose | V2 | https://docs.docker.com/compose/reference/ |
| Ruby | latest stable | https://www.ruby-lang.org/en/ |
| Bundler | latest satble | https://bundler.io/ |

Please fork this repository and check out the codes.

```
$ git clone git@github.com:your-account-name/redis-cluster-client.git
$ cd redis-cluster-client/
$ git remote add upstream https://github.com/redis-rb/redis-cluster-client.git
$ git fetch -p upstream
```

Please install libraries.

```
$ bundle install --path=.bundle --jobs=4
```

Please run Redis cluster with Docker.

```
## If you use Docker server and your OS is Linux:
$ docker compose up

## else:
$ HOST_ADDR=192.168.xxx.xxx docker compose -f compose.nat.yaml up
$ DEBUG=1 bundle exec rake 'build_cluster[192.168.xxx.xxx]'

### When the above rake task is not working:
$ docker compose -f compose.nat.yaml exec node1 bash -c "yes yes | redis-cli --cluster create --cluster-replicas 1 $(seq 6379 6384 | xargs -I {} echo 192.168.xxx.xxx:{} | xargs echo)"
```

Please run basic test cases.

```
$ bundle exec rake test
```

You can see more information in the YAML file for GItHub actions.

## See also
* https://github.com/redis/redis-rb/issues/1070
* https://github.com/redis/redis/issues/8948
* https://github.com/antirez/redis-rb-cluster
