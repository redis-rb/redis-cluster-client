# frozen_string_literal: true

require 'uri'
require 'redis_client'
require 'redis_client/cluster'
require 'redis_client/cluster/node_key'

class RedisClient
  class ClusterConfig
    DEFAULT_NODES = %w[redis://127.0.0.1:6379].freeze
    FIXED_DB_INDEX = 0
    DEFAULT_SCHEME = 'redis'
    SECURE_SCHEME = 'rediss'
    VALID_SCHEMES = [DEFAULT_SCHEME, SECURE_SCHEME].freeze
    VALID_NODES_KEYS = %i[host port ssl username password].freeze

    InvalidClientConfigError = Class.new(::RedisClient::Error)

    def initialize(nodes: DEFAULT_NODES, replica: false, fixed_hostname: nil, **client_config)
      @replica = true & replica
      @fixed_hostname = fixed_hostname.to_s
      @client_config = client_config
      @node_configs = build_node_configs(nodes.dup)
      add_common_node_config_if_needed(@client_config, @node_configs, :ssl)
      add_common_node_config_if_needed(@client_config, @node_configs, :username)
      add_common_node_config_if_needed(@client_config, @node_configs, :password)
    end

    def inspect
      per_node_key.to_s
    end

    def new_pool(size: 5, timeout: 5, **kwargs)
      ::RedisClient::Cluster.new(self, pool: { size: size, timeout: timeout }, **kwargs)
    end

    def new_client(**kwargs)
      ::RedisClient::Cluster.new(self, **kwargs)
    end

    def per_node_key
      @node_configs.to_h do |config|
        node_key = ::RedisClient::Cluster::NodeKey.build_from_host_port(config[:host], config[:port])
        config = @client_config.merge(config)
        config = config.merge(host: @fixed_hostname) unless @fixed_hostname.empty?
        config[:db] = FIXED_DB_INDEX
        [node_key, config]
      end
    end

    def use_replica?
      @replica
    end

    def update_node(addrs)
      @node_configs = build_node_configs(addrs)
    end

    def add_node(host, port)
      @node_configs << { host: host, port: port }
    end

    def dup
      self.class.new(nodes: @node_configs, replica: @replica, fixed_hostname: @fixed_hostname, **@client_config)
    end

    private

    def build_node_configs(addrs)
      configs = Array[addrs].flatten.map { |addr| parse_node_addr(addr) }
      raise InvalidClientConfigError, '`nodes` option is empty' if configs.size.zero?

      configs
    end

    def parse_node_addr(addr)
      case addr
      when String
        parse_node_url(addr)
      when Hash
        parse_node_option(addr)
      else
        raise InvalidClientConfigError, '`nodes` option includes invalid type values'
      end
    end

    def parse_node_url(addr) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      uri = URI(addr)
      raise InvalidClientConfigError, "`nodes` option includes a invalid uri scheme: #{addr}" unless VALID_SCHEMES.include?(uri.scheme)

      username = uri.user ? URI.decode_www_form_component(uri.user) : nil
      password = uri.password ? URI.decode_www_form_component(uri.password) : nil

      { host: uri.host, port: uri.port, username: username, password: password, ssl: uri.scheme == SECURE_SCHEME }
        .reject { |_, v| v.nil? || v == '' || v == false }
    rescue URI::InvalidURIError => e
      raise InvalidClientConfigError, e.message
    end

    def parse_node_option(addr)
      addr = addr.transform_keys(&:to_sym)
      raise InvalidClientConfigError, '`nodes` option does not include `:host` and `:port` keys' if addr.values_at(:host, :port).any?(&:nil?)

      begin
        addr[:port] = Integer(addr[:port])
      rescue ArgumentError => e
        raise InvalidClientConfigError, e.message
      end

      addr.select { |k, _| VALID_NODES_KEYS.include?(k) }
    end

    def add_common_node_config_if_needed(client_config, node_configs, key)
      return client_config if client_config[key].nil? && (node_configs.size.zero? || node_configs.first[key].nil?)

      client_config[key] ||= node_configs.first[key]
    end
  end
end
