[![Gem Version](https://badge.fury.io/rb/redis-cluster-client.svg)](https://badge.fury.io/rb/redis-cluster-client)
![Test status](https://github.com/redis-rb/redis-cluster-client/workflows/Test/badge.svg?branch=master)

Redis Cluster Client
===============================================================================
This library is a client for [Redis cluster](https://redis.io/docs/reference/cluster-spec/).
It depends on [redis-client](https://github.com/redis-rb/redis-client).
So it would be better to read `redis-client` documents first.

## Background
This gem is underlying in the official gem which is named as [redis-clustering](https://rubygems.org/gems/redis-clustering).
The redis-clustering gem was decoupled from [the redis gem](https://rubygems.org/gems/redis) since `v5` or later.
Both are maintained by [the repository](https://github.com/redis/redis-rb) in the official organization.
The redis gem supported cluster mode since [the pull request](https://github.com/redis/redis-rb/pull/716) was merged until `v4`.
You can see more details and reasons in [the issue](https://github.com/redis/redis-rb/issues/1070) if you have interest.

## Installation
```ruby
gem 'redis-cluster-client'
```

## Initialization
| key | type | default | description |
| --- | --- | --- | --- |
| `:nodes` | String or Hash or Array<String, Hash> | `['redis://127.0.0.1:6379']` | node addresses for startup connection |
| `:replica` | Boolean | `false` | `true` if client should use scale read feature |
| `:replica_affinity` | Symbol or String | `:random` | scale reading strategy, `:random`, `random_with_primary` or `:latency` are valid |
| `:fixed_hostname` | String | `nil` | required if client should connect to single endpoint with SSL |
| `:slow_command_timeout` | Integer | `-1` | timeout used for "slow" queries that fetch metdata e.g. CLUSTER NODES, COMMAND |
| `:concurrency` | Hash | `{ model: :on_demand, size: 5}` | concurrency settings, `:on_demand`, `:pooled` and `:none` are valid models, size is a max number of workers, `:none` model is no concurrency, Please choose the one suited your environment if needed. |
| `:connect_with_original_config` | Boolean | `false` | `true` if client should retry the connection using the original endpoint that was passed in |

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

# To connect to all nodes to use scale reading feature + make reads equally likely from replicas and primary
RedisClient.cluster(replica: true, replica_affinity: :random_with_primary).new_client
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

## with URL:
### User name and password should be URI encoded and the same in every node.
username = 'myuser'
password = URI.encode_www_form_component('!&<123-abc>')
RedisClient.cluster(nodes: %W[redis://#{username}:#{password}@node1:6379 redis://#{username}:#{password}@node2:6379]).new_client

## with options:
RedisClient.cluster(nodes: %w[redis://node1:6379 redis://node2:6379], username: 'myuser', password: '!&<123-abc>').new_client
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

```ruby
# To specify a timeout for "slow" commands (CLUSTER NODES, COMMAND)
RedisClient.cluster(slow_command_timeout: 4).new_client
```

```ruby
# To specify concurrency settings
RedisClient.cluster(concurrency: { model: :on_demand, size: 6 }).new_client
RedisClient.cluster(concurrency: { model: :pooled, size: 3 }).new_client
RedisClient.cluster(concurrency: { model: :none }).new_client

# The above settings are used by sending commands to multiple nodes like pipelining.
# Please choose the one suited your workloads.
```

```ruby
# To reconnect using the original configuration options on error. This can be useful when using a DNS endpoint and the underlying host IPs are all updated
RedisClient.cluster(connect_with_original_config: true).new_client
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
* `#multi`
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

## Transactions
This gem supports [Redis transactions](https://redis.io/topics/transactions), including atomicity with `MULTI`/`EXEC`,
and conditional execution with `WATCH`. Redis does not support cross-node transactions, so all keys used within a
transaction must live in the same key slot. To use transactions, you must thus "pin" your client to a single connection using
`#with`. You can pass a single key, in order to perform multiple operations atomically on the same key, like so:

```ruby
cli.with(key: 'my_cool_key') do |conn|
  conn.multi do |m|
    m.call('INC', 'my_cool_key')
    m.call('INC', 'my_cool_key')
  end
  # my_cool_key will be incremented by 2, with no intermediate state visible to other clients
end
```

More commonly, however, you will want to perform transactions across multiple keys. To do this, you need to ensure that all keys used in the transaction hash to the same slot; Redis a mechanism called [hashtags](https://redis.io/docs/reference/cluster-spec/#hash-tags) to achieve this. If a key contains a hashag (e.g. in the key `{foo}bar`, the hashtag is `foo`), then it is guaranted to hash to the same slot (and thus always live on the same node) as other keys which contain the same hashtag.

So, whilst it's not possible in Redis cluster to perform a transction on the keys `foo` and `bar`, it _is_ possible to perform a transaction on the keys `{tag}foo` and `{tag}bar`. To perform such transactions on this gem, pass `hashtag:` to `#with` instead of `key`:

```ruby
cli.with(hashtag: 'user123') do |conn|
  # You can use any key which contains "{user123}" in this block
  conn.multi do |m|
    m.call('INC', '{user123}coins_spent')
    m.call('DEC', '{user123}coins_available')
  end
end
```

Once you have pinned a client to a particular slot, you can use the same transaction APIs as the
[redis-client](https://github.com/redis-rb/redis-client#usage) gem allows.
```ruby
# No concurrent client will ever see the value 1 in 'mykey'; it will see either zero or two.
cli.call('SET', 'key', 0)
cli.with(key: 'key') do |conn|
  conn.multi do |txn|
    txn.call('INCR', 'key')
    txn.call('INCR', 'key')
  end
  #=> ['OK', 'OK']
end
# Conditional execution with WATCH can be used to e.g. atomically swap two keys
cli.call('MSET', '{myslot}1', 'v1', '{myslot}2', 'v2')
cli.with(hashtag: 'myslot') do |conn|
  conn.call('WATCH', '{myslot}1', '{myslot}2')
  conn.multi do |txn|
    old_key1 = conn.call('GET', '{myslot}1')
    old_key2 = conn.call('GET', '{myslot}2')
    txn.call('SET', '{myslot}1', old_key2)
    txn.call('SET', '{myslot}2', old_key1)
  end
  # This transaction will swap the values of {myslot}1 and {myslot}2 only if no concurrent connection modified
  # either of the values
end
# You can also pass watch: to #multi as a shortcut
cli.call('MSET', '{myslot}1', 'v1', '{myslot}2', 'v2')
cli.with(hashtag: 'myslot') do |conn|
  conn.multi(watch: ['{myslot}1', '{myslot}2']) do |txn|
    old_key1, old_key2 = conn.call('MGET', '{myslot}1', '{myslot}2')
    txn.call('MSET', '{myslot}1', old_key2, '{myslot}2', old_key1)
  end
end
```

Pinned connections are aware of redirections and node failures like ordinary calls to `RedisClient::Cluster`, but because
you may have written non-idempotent code inside your block, the block is not automatically retried if e.g. the slot
it is operating on moves to a different node. If you want this, you can opt-in to retries by passing nonzero
`retry_count` to `#with`.
```ruby
cli.with(hashtag: 'myslot', retry_count: 1) do |conn|
  conn.call('GET', '{myslot}1')
  #=> "value1"
  # Now, some changes in cluster topology mean that {key} is moved to a different node!
  conn.call('GET', '{myslot}2')
  #=> MOVED 9039 127.0.0.1:16381 (RedisClient::CommandError)
  # Luckily, the block will get retried (once) and so both GETs will be re-executed on the newly-discovered
  # correct node.
end
```

Because `RedisClient` from the redis-client gem implements `#with` as simply `yield self` and ignores all of its
arguments, it's possible to write code which is compatible with both redis-client and redis-cluster-client; the `#with`
call will pin the connection to a slot when using clustering, or be a no-op when not.

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

## Migration
This library might help you if you want to migrate your Redis from a standalone server to a cluster.
Here is an example code.

```ruby
# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'redis-cluster-client'
end

src = RedisClient.config(url: ENV.fetch('REDIS_URL')).new_client
dest = RedisClient.cluster(nodes: ENV.fetch('REDIS_CLUSTER_URL')).new_client
node = dest.instance_variable_get(:@router).instance_variable_get(:@node)

src.scan do |key|
  slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
  node_key = node.find_node_key_of_primary(slot)
  host, port = ::RedisClient::Cluster::NodeKey.split(node_key)
  src.blocking_call(10, 'MIGRATE', host, port, key, 0, 7, 'COPY', 'REPLACE')
end
```

It needs more enhancement to be enough performance in the production environment that has tons of keys.
Also, it should handle errors.

## See also
* https://redis.io/docs/reference/cluster-spec/
* https://github.com/redis/redis-rb/issues/1070
* https://github.com/redis/redis/issues/8948
* https://github.com/antirez/redis-rb-cluster
* https://twitter.com/antirez
* http://antirez.com/latest/0
* https://www.youtube.com/@antirez
* https://www.twitch.tv/thetrueantirez/
