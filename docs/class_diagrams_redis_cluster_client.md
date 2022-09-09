# RedisClient::Cluster

```mermaid
classDiagram
  class RedisClient_Cluster {
    +inspect()
    +call()
    +call_v()
    +call_once()
    +call_once_v()
    +blocking_call()
    +blocking_call_v()
    +scan()
    +sscan()
    +hscan()
    +zscan()
    +pipelined()
    +pubsub()
    +close()
  }

  class RedisClient_ClusterConfig {
    +inspect()
    +new_pool()
    +new_client()
    +per_node_key()
    +use_replica?()
    +update_node()
    +add_node()
    +dup()
  }

  class RedisClient_Cluster_Command {
    +self.load()
    +extract_first_key()
    +should_send_to_primary?()
    +should_send_to_replica?()
  }

  class module_RedisClient_Cluster_KeySlotConverter {
    +convert()
  }

  class RedisClient_Cluster_Node {
    +self.load_info()
    +inspect()
    +each()
    +sample()
    +node_keys()
    +find_by()
    +call_all()
    +call_primaries()
    +call_replicas()
    +send_ping()
    +clients_for_scanning()
    +find_node_key_of_primary()
    +find_node_key_of_replica()
    +any_primary_node_key()
    +any_replica_node_key()
    +update_slot()
  }

  class RedisClient_Cluster_Node_PrimaryOnly {
    +clients()
    +primary_clients()
    +replica_clients()
    +clients_for_scanning()
    +find_node_key_of_replica()
    +any_primary_node_key()
    +any_replica_node_key()
  }

  class RedisClient_Cluster_Node_RandomReplica {
    +replica_clients()
    +clients_for_scanning()
    +find_node_key_of_replica()
    +any_replica_node_key()
  }

  class RedisClient_Cluster_Node_LatencyReplica {
    +replica_clients()
    +clients_for_scanning()
    +find_node_key_of_replica()
    +any_replica_node_key()
  }

  class module_RedisClient_Cluster_Node_ReplicaMixin {
    +clients()
    +primary_clients()
    +any_primary_node_key()
  }

  class module_RedisClient_Cluster_NodeKey {
    +hashify()
    +split()
    +build_from_uri()
    +build_from_host_port()
  }

  class RedisClient_Cluster_Pipeline {
    +call()
    +call_once()
    +blocking_call()
    +empty?()
    +execute()
  }

  class RedisClient_Cluster_PubSub {
    +call()
    +close()
    +next_event()
  }

  class RedisClient_Cluster_Router {
    +send_command()
    +try_send()
    +scan()
    +assign_node()
    +find_node_key()
    +find_node()
  }

  RedisClient_ClusterConfig ..> RedisClient_Cluster : new

  RedisClient_Cluster ..> RedisClient_Cluster_Pipeline : new
  RedisClient_Cluster ..> RedisClient_Cluster_PubSub : new
  RedisClient_Cluster ..> RedisClient_Cluster_Router : new

  RedisClient_Cluster_Pipeline ..> RedisClient_Cluster_Router : use
  RedisClient_Cluster_PubSub ..> RedisClient_Cluster_Router : use

  RedisClient_Cluster_Router ..> RedisClient_Cluster_Node : new
  RedisClient_Cluster_Router ..> RedisClient_Cluster_Command : new
  RedisClient_Cluster_Router ..> module_RedisClient_Cluster_KeySlotConverter : call
  RedisClient_Cluster_Router ..> module_RedisClient_Cluster_NodeKey : call

  RedisClient_Cluster_Node_RandomReplica ..|> module_RedisClient_Cluster_Node_ReplicaMixin : include
  RedisClient_Cluster_Node_LatencyReplica ..|> module_RedisClient_Cluster_Node_ReplicaMixin : include
  RedisClient_Cluster_Node ..> RedisClient_Cluster_Node_PrimaryOnly : new
  RedisClient_Cluster_Node ..> RedisClient_Cluster_Node_RandomReplica : new
  RedisClient_Cluster_Node ..> RedisClient_Cluster_Node_LatencyReplica : new
```
