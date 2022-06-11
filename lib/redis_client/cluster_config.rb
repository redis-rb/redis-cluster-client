# frozen_string_literal: true

require 'uri'
require 'redis_client'
require 'redis_client/config'
require 'redis_client/cluster'
require 'redis_client/cluster/node_key'

class RedisClient
  class ClusterConfig
    include ::RedisClient::Config::Common

    DEFAULT_SCHEME = 'redis'
    SECURE_SCHEME = 'rediss'
    VALID_SCHEMES = [DEFAULT_SCHEME, SECURE_SCHEME].freeze
    InvalidClientConfigError = Class.new(::RedisClient::Error)

    def initialize(nodes:, replica: false, fixed_hostname: nil, **client_config)
      @node_configs = build_node_options(nodes.dup)
      @replica = replica
      @fixed_hostname = fixed_hostname
      @client_config = client_config.dup
      add_common_node_config_if_needed(@client_config, @node_configs, :scheme)
      add_common_node_config_if_needed(@client_config, @node_configs, :username)
      add_common_node_config_if_needed(@client_config, @node_configs, :password)
      super(**@client_config)
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
        config = config.merge(host: @fixed_hostname) if @fixed_hostname && !@fixed_hostname.empty?
        [node_key, config]
      end
    end

    def use_replica?
      @replica
    end

    def update_node(addrs)
      @node_configs = build_node_options(addrs)
    end

    def add_node(host, port)
      @node_configs << { host: host, port: port }
    end

    def dup
      self.class.new(nodes: @node_configs, replica: @replica, fixed_hostname: @fixed_hostname, **@client_config)
    end

    private

    def build_node_options(addrs)
      raise InvalidClientConfigError, 'Redis option of `cluster` must be an Array' unless addrs.is_a?(Array)

      addrs.map { |addr| parse_node_addr(addr) }
    end

    def parse_node_addr(addr)
      case addr
      when String
        parse_node_url(addr)
      when Hash
        parse_node_option(addr)
      else
        raise InvalidClientConfigError, 'Redis option of `cluster` must includes String or Hash'
      end
    end

    def parse_node_url(addr) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      uri = URI(addr)
      raise InvalidClientConfigError, "Invalid uri scheme #{addr}" unless VALID_SCHEMES.include?(uri.scheme)

      db = uri.path.split('/')[1]&.to_i
      username = uri.user ? URI.decode_www_form_component(uri.user) : nil
      password = uri.password ? URI.decode_www_form_component(uri.password) : nil

      { scheme: uri.scheme, username: username, password: password, host: uri.host, port: uri.port, db: db }.reject { |_, v| v.nil? || v == '' }
    rescue URI::InvalidURIError => e
      raise InvalidClientConfigError, e.message
    end

    def parse_node_option(addr)
      addr = addr.transform_keys(&:to_sym)
      raise InvalidClientConfigError, 'Redis option of `cluster` must includes `:host` and `:port` keys' if addr.values_at(:host, :port).any?(&:nil?)

      addr
    end

    def add_common_node_config_if_needed(client_config, node_configs, key)
      return client_config if client_config[key].nil? && node_configs.first[key].nil?

      client_config[key] ||= node_configs.first[key]
    end
  end
end
