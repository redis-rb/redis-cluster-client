# frozen_string_literal: true

class RedisClient
  class Cluster
    # Node key's format is `<ip>:<port>`.
    # It is different from node id.
    # Node id is internal identifying code in Redis Cluster.
    module NodeKey
      DELIMITER = ':'

      module_function

      def hashify(node_key)
        host, port = split(node_key)
        { host: host, port: port }
      end

      def split(node_key)
        pos = node_key&.rindex(DELIMITER, -1)
        return [node_key, nil] if pos.nil?

        [node_key[0, pos], node_key[(pos + 1)..]]
      end

      def build_from_uri(uri)
        return '' if uri.nil?

        "#{uri.host}#{DELIMITER}#{uri.port}"
      end

      def build_from_host_port(host, port)
        "#{host}#{DELIMITER}#{port}"
      end
    end
  end
end
