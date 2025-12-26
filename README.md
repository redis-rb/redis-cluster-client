[![Gem Version](https://badge.fury.io/rb/redis-cluster-client.svg)](https://badge.fury.io/rb/redis-cluster-client)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/redis-rb/redis-cluster-client)
![Test status](https://github.com/redis-rb/redis-cluster-client/actions/workflows/test.yaml/badge.svg?branch=master)
![Release status](https://github.com/redis-rb/redis-cluster-client/actions/workflows/release.yaml/badge.svg)

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
| `:concurrency` | Hash | `{ model: :none }` | concurrency settings, `:on_demand`, `:pooled`, `:none`, and `actor`  are valid models, size is a max number of workers, `:none` model is no concurrency, Please choose the one suited your environment if needed. |
| `:connect_with_original_config` | Boolean | `false` | `true` if client should retry the connection using the original endpoint that was passed in |
| `:max_startup_sample` | Integer | `3` | maximum number of nodes to fetch `CLUSTER NODES` information for startup |

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
RedisClient.cluster(concurrency: { model: :actor }).new_client

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

The `#scan` method iterates all keys around every node seamlessly.
The `#pipelined` method splits and sends commands to each node and aggregates replies.
The `#multi` method supports the transaction feature but you should use a hashtag for your keys.
The `#pubsub` method supports sharded subscriptions.
Every interface handles redirections and resharding states internally.

## Multiple keys and CROSSSLOT error
A subset of commands can be passed multiple keys.
In cluster mode, these commands have a constraint that passed keys should belong to the same slot
and not just the same node.
Therefore, the following error occurs:

```
$ redis-cli -c mget key1 key2 key3
(error) CROSSSLOT Keys in request don't hash to the same slot

$ redis-cli -c cluster keyslot key1
(integer) 9189

$ redis-cli -c cluster keyslot key2
(integer) 4998

$ redis-cli -c cluster keyslot key3
(integer) 935
```

For the constraint, Redis cluster provides a feature to be able to bias keys to the same slot with a hash tag.

```
$ redis-cli -c mget {key}1 {key}2 {key}3
1) (nil)
2) (nil)
3) (nil)

$ redis-cli -c cluster keyslot {key}1
(integer) 12539

$ redis-cli -c cluster keyslot {key}2
(integer) 12539

$ redis-cli -c cluster keyslot {key}3
(integer) 12539
```

In addition, this gem works multiple keys without a hash tag in MGET, MSET and DEL commands
using pipelining internally automatically.
If the first key includes a hash tag, this gem sends the command to the node as is.
If the first key doesn't have a hash tag, this gem converts the command into single-key commands
and sends them to nodes with pipelining, then gathering replies and returning them.

```ruby
r = RedisClient.cluster.new_client
#=> #<RedisClient::Cluster 127.0.0.1:6379>

r.call('mget', 'key1', 'key2', 'key3')
#=> [nil, nil, nil]

r.call('mget', '{key}1', '{key}2', '{key}3')
#=> [nil, nil, nil]
```

This behavior is for upper libraries to be able to keep a compatibility with a standalone client.
You can exploit this behavior for migrating from a standalone server to a cluster.
Although multiple-time queries with single-key commands are slower than pipelining,
that pipelined queries are slower than a single-slot query with multiple keys.
Hence, we recommend to use a hash tag in this use case for the better performance.

## Transactions
This gem supports [Redis transactions](https://redis.io/topics/transactions), including atomicity with `MULTI`/`EXEC`,
and conditional execution with `WATCH`. Redis does not support cross-node transactions, so all keys used within a
transaction must live in the same key slot. To use transactions, you can use `#multi` method same as the [redis-client](https://github.com/redis-rb/redis-client#usage):

```ruby
cli.multi do |tx|
  tx.call('INCR', 'my_key')
  tx.call('INCR', 'my_key')
end
```

More commonly, however, you will want to perform transactions across multiple keys. To do this,
you need to ensure that all keys used in the transaction hash to the same slot;
Redis a mechanism called [hashtags](https://redis.io/docs/reference/cluster-spec/#hash-tags) to achieve this.
If a key contains a hashag (e.g. in the key `{foo}bar`, the hashtag is `foo`),
then it is guaranted to hash to the same slot (and thus always live on the same node) as other keys which contain the same hashtag.

So, whilst it's not possible in Redis cluster to perform a transction on the keys `foo` and `bar`,
it _is_ possible to perform a transaction on the keys `{tag}foo` and `{tag}bar`.
To perform such transactions on this gem, use the hashtag:

```ruby
cli.multi do |tx|
  tx.call('INCR', '{user123}coins_spent')
  tx.call('DECR', '{user123}coins_available')
end
```

```ruby
# Conditional execution with WATCH can be used to e.g. atomically swap two keys
cli.call('MSET', '{myslot}1', 'v1', '{myslot}2', 'v2')
cli.multi(watch: %w[{myslot}1 {myslot}2]) do |tx|
  old_key1 = cli.call('GET', '{myslot}1')
  old_key2 = cli.call('GET', '{myslot}2')
  tx.call('SET', '{myslot}1', old_key2)
  tx.call('SET', '{myslot}2', old_key1)
end
# This transaction will swap the values of {myslot}1 and {myslot}2 only if no concurrent connection modified
# either of the values
```

You can early return out of your block with a `next` statement if you want to cancel your transaction.
In this context, don't use `break` and `return` statements.

```ruby
# The transaction isn't executed.
cli.multi do |tx|
  next if some_conditions?

  tx.call('SET', '{key}1', '1')
  tx.call('SET', '{key}2', '2')
end
```

```ruby
# The watching state is automatically cleared with an execution of an empty transaction.
cli.multi(watch: %w[{key}1 {key}2]) do |tx|
  next if some_conditions?

  tx.call('SET', '{key}1', '1')
  tx.call('SET', '{key}2', '2')
end
```

`RedisClient::Cluster#multi` is aware of redirections and node failures like ordinary calls to `RedisClient::Cluster`,
but because you may have written non-idempotent code inside your block, the block is called once if e.g. the slot
it is operating on moves to a different node.

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
| Ruby | latest stable | https://www.ruby-lang.org/en/ |

Please fork this repository and check out the codes.

```
$ git clone git@github.com:your-account-name/redis-cluster-client.git
$ cd redis-cluster-client/
$ git remote add upstream https://github.com/redis-rb/redis-cluster-client.git
$ git fetch -p upstream
```

Please do the following steps.

* Build a Redis cluster with Docker
* Install gems
* Run basic test cases

```
## If you use Docker server and your OS is Linux:
$ bundle config set path '.bundle'
$ bundle install --jobs=$(grep process /proc/cpuinfo | wc -l)
$ docker compose up
$ bundle exec rake test

## else:
$ docker compose --profile ruby up
$ docker compose --profile ruby exec ruby bundle install
$ docker compose --profile ruby exec ruby bundle exec rake test
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
* https://github.com/valkey-io/valkey/issues/384
* https://github.com/antirez/redis-rb-cluster
* https://twitter.com/antirez
* https://bsky.app/profile/antirez.bsky.social
* http://antirez.com/latest/0
* https://www.youtube.com/@antirez
* https://www.twitch.tv/thetrueantirez/
