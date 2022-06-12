# Architectures of Redis cluster with SSL/TLS as imagined

## AWS
The client can connect to the nodes directly.
The endpoint is just a CNAME record of DNS.
It is as simple as that.

```mermaid
graph TB
  client(Cluster Client)

  subgraph Amazon ElastiCache for Redis
    node0(Node0)
    node1(Node1)
    node2(Node2)
  end

  node0-.-node1-.-node2-.-node0
  client--rediss://node0.example.com:6379-->node0
  client--rediss://node1.example.com:6379-->node1
  client--rediss://node2.example.com:6379-->node2
```

## Microsoft Azure
The service provides a single IP address and multiple ports mapped to each node.
The endpoint doesn't support redirection.
It does only SSL/TLS termination.

```mermaid
graph TB
  client(Cluster Client)

  subgraph Azure Cache for Redis
    subgraph Endpoint
      endpoint(Active)
      endpoint_sb(Standby)
    end

    subgraph Cluster
      node0(Node0)
      node1(Node1)
      node2(Node2)
    end
  end

  endpoint-.-endpoint_sb
  node0-.-node1-.-node2-.-node0
  client--rediss://endpoint.example.com:15000-->endpoint--redis://node0:6379-->node0
  client--rediss://endpoint.example.com:15001-->endpoint--redis://node1:6379-->node1
  client--rediss://endpoint.example.com:15002-->endpoint--redis://node2:6379-->node2
```
