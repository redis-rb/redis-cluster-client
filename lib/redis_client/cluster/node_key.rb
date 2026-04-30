# frozen_string_literal: true

class RedisClient
  class Cluster
    # Node key's format is `<ip>:<port>`.
    # It is different from node id.
    # Node id is internal identifying code in Redis Cluster.
    module NodeKey
      DELIMITER = ':'

      private_constant :DELIMITER

      module_function

      def hashify(node_key)
        host, port = split(node_key)
        { host: host, port: port }
      end

      def split(node_key)
        return [node_key, nil] if node_key.nil? || node_key.empty?

        bracketed = split_bracketed(node_key)
        return bracketed unless bracketed.nil?

        pos = node_key.rindex(DELIMITER, -1)
        return [node_key, nil] if pos.nil?

        [node_key[0, pos], node_key[(pos + 1)..]]
      end

      def split_bracketed(node_key)
        return nil unless node_key.start_with?('[')

        end_bracket = node_key.index(']')
        return nil if end_bracket.nil?

        host = node_key[1, end_bracket - 1]
        remainder = node_key[(end_bracket + 1)..]
        port = remainder.start_with?(DELIMITER) ? remainder[1..] : nil
        [host, port]
      end
      private_class_method :split_bracketed

      def build_from_uri(uri)
        return '' if uri.nil?

        "#{uri.host}#{DELIMITER}#{uri.port}"
      end

      def build_from_host_port(host, port)
        "#{host}#{DELIMITER}#{port}"
      end

      def build_from_client(client)
        "#{client.config.host}#{DELIMITER}#{client.config.port}"
      end
    end
  end
end
