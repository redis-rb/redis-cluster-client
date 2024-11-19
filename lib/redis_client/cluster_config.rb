# frozen_string_literal: true

require 'uri'
require 'redis_client'
require 'redis_client/cluster'
require 'redis_client/cluster/errors'
require 'redis_client/cluster/node_key'
require 'redis_client/cluster/noop_command_builder'
require 'redis_client/command_builder'

class RedisClient
  class ClusterConfig
    DEFAULT_HOST = '127.0.0.1'
    DEFAULT_PORT = 6379
    DEFAULT_SCHEME = 'redis'
    SECURE_SCHEME = 'rediss'
    DEFAULT_NODES = ["#{DEFAULT_SCHEME}://#{DEFAULT_HOST}:#{DEFAULT_PORT}"].freeze
    VALID_SCHEMES = [DEFAULT_SCHEME, SECURE_SCHEME].freeze
    VALID_NODES_KEYS = %i[ssl username password host port db].freeze
    MERGE_CONFIG_KEYS = %i[ssl username password].freeze
    IGNORE_GENERIC_CONFIG_KEYS = %i[url host port path].freeze
    MAX_WORKERS = Integer(ENV.fetch('REDIS_CLIENT_MAX_THREADS', -1)) # for backward compatibility
    # It's used with slow queries of fetching meta data like CLUSTER NODES, COMMAND and so on.
    SLOW_COMMAND_TIMEOUT = Float(ENV.fetch('REDIS_CLIENT_SLOW_COMMAND_TIMEOUT', -1))
    # It affects to strike a balance between load and stability in initialization or changed states.
    MAX_STARTUP_SAMPLE = Integer(ENV.fetch('REDIS_CLIENT_MAX_STARTUP_SAMPLE', 3))

    private_constant :DEFAULT_HOST, :DEFAULT_PORT, :DEFAULT_SCHEME, :SECURE_SCHEME, :DEFAULT_NODES,
                     :VALID_SCHEMES, :VALID_NODES_KEYS, :MERGE_CONFIG_KEYS, :IGNORE_GENERIC_CONFIG_KEYS,
                     :MAX_WORKERS, :SLOW_COMMAND_TIMEOUT, :MAX_STARTUP_SAMPLE

    InvalidClientConfigError = Class.new(::RedisClient::Cluster::Error)

    attr_reader :command_builder, :client_config, :replica_affinity, :slow_command_timeout,
                :connect_with_original_config, :startup_nodes, :max_startup_sample, :id

    def initialize( # rubocop:disable Metrics/ParameterLists
      nodes: DEFAULT_NODES,
      replica: false,
      replica_affinity: :random,
      fixed_hostname: '',
      concurrency: nil,
      connect_with_original_config: false,
      client_implementation: ::RedisClient::Cluster, # for redis gem
      slow_command_timeout: SLOW_COMMAND_TIMEOUT,
      command_builder: ::RedisClient::CommandBuilder,
      max_startup_sample: MAX_STARTUP_SAMPLE,
      **client_config
    )

      @replica = true & replica
      @replica_affinity = replica_affinity.to_s.to_sym
      @fixed_hostname = fixed_hostname.to_s
      @command_builder = command_builder
      node_configs = build_node_configs(nodes.dup)
      @client_config = merge_generic_config(client_config, node_configs)
      # Keep tabs on the original startup nodes we were constructed with
      @startup_nodes = build_startup_nodes(node_configs)
      @concurrency = merge_concurrency_option(concurrency)
      @connect_with_original_config = connect_with_original_config
      @client_implementation = client_implementation
      @slow_command_timeout = slow_command_timeout
      @max_startup_sample = max_startup_sample
      @id = client_config[:id]
    end

    def inspect
      "#<#{self.class.name} #{startup_nodes.values.map { |v| v.reject { |k| k == :command_builder } }}>"
    end

    def read_timeout
      @client_config[:read_timeout] || @client_config[:timeout] || ::RedisClient::Config::DEFAULT_TIMEOUT
    end

    def new_pool(size: 5, timeout: 5, **kwargs)
      @client_implementation.new(
        self,
        pool: { size: size, timeout: timeout },
        concurrency: @concurrency,
        **kwargs
      )
    end

    def new_client(**kwargs)
      @client_implementation.new(self, concurrency: @concurrency, **kwargs)
    end

    def use_replica?
      @replica
    end

    def client_config_for_node(node_key)
      config = ::RedisClient::Cluster::NodeKey.hashify(node_key)
      config[:port] = ensure_integer(config[:port])
      augment_client_config(config)
    end

    def resolved?
      true
    end

    def sentinel?
      false
    end

    def server_url
      nil
    end

    private

    def merge_concurrency_option(option)
      opts = {}

      if MAX_WORKERS.positive?
        opts[:model] = :on_demand
        opts[:size] = MAX_WORKERS
      end

      opts.merge!(option.transform_keys(&:to_sym)) if option.is_a?(Hash)
      opts[:model] = :none if opts.empty?
      opts.freeze
    end

    def build_node_configs(addrs)
      configs = Array[addrs].flatten.filter_map { |addr| parse_node_addr(addr) }
      raise InvalidClientConfigError, '`nodes` option is empty' if configs.empty?

      configs
    end

    def parse_node_addr(addr)
      case addr
      when String
        parse_node_url(addr)
      when Hash
        parse_node_option(addr)
      else
        raise InvalidClientConfigError, "`nodes` option includes invalid type values: #{addr}"
      end
    end

    def parse_node_url(addr) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
      return if addr.empty?

      uri = URI(addr)
      scheme = uri.scheme || DEFAULT_SCHEME
      raise InvalidClientConfigError, "`nodes` option includes a invalid uri scheme: #{addr}" unless VALID_SCHEMES.include?(scheme)

      username = uri.user ? URI.decode_www_form_component(uri.user) : nil
      password = uri.password ? URI.decode_www_form_component(uri.password) : nil
      host = uri.host || DEFAULT_HOST
      port = uri.port || DEFAULT_PORT
      db = uri.path.index('/').nil? ? uri.path : uri.path.split('/')[1]
      db = db.nil? || db.empty? ? db : ensure_integer(db)

      { ssl: scheme == SECURE_SCHEME, username: username, password: password, host: host, port: port, db: db }
        .reject { |_, v| v.nil? || v == '' || v == false }
    rescue URI::InvalidURIError => e
      raise InvalidClientConfigError, "#{e.message}: #{addr}"
    end

    def parse_node_option(addr)
      return if addr.empty?

      addr = addr.transform_keys(&:to_sym)
      addr[:host] ||= DEFAULT_HOST
      addr[:port] = ensure_integer(addr[:port] || DEFAULT_PORT)
      addr.select { |k, _| VALID_NODES_KEYS.include?(k) }
    end

    def ensure_integer(value)
      Integer(value)
    rescue ArgumentError => e
      raise InvalidClientConfigError, e.message
    end

    def merge_generic_config(client_config, node_configs)
      cfg = node_configs.first || {}
      client_config.reject { |k, _| IGNORE_GENERIC_CONFIG_KEYS.include?(k) }
                   .merge(cfg.slice(*MERGE_CONFIG_KEYS))
    end

    def build_startup_nodes(configs)
      configs.to_h do |config|
        node_key = ::RedisClient::Cluster::NodeKey.build_from_host_port(config[:host], config[:port])
        config = augment_client_config(config)
        [node_key, config]
      end
    end

    def augment_client_config(config)
      config = @client_config.merge(config)
      config = config.merge(host: @fixed_hostname) unless @fixed_hostname.empty?
      config[:command_builder] = ::RedisClient::Cluster::NoopCommandBuilder # prevent twice call
      config
    end
  end
end
