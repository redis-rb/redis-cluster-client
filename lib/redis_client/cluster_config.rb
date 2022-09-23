# frozen_string_literal: true

require 'uri'
require 'redis_client'
require 'redis_client/cluster'
require 'redis_client/cluster/node_key'
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

    InvalidClientConfigError = Class.new(::RedisClient::Error)

    attr_reader :command_builder, :client_config, :replica_affinity

    def initialize(
      nodes: DEFAULT_NODES,
      replica: false,
      replica_affinity: :random,
      fixed_hostname: '',
      client_implementation: Cluster,
      **client_config
    )

      @replica = true & replica
      @replica_affinity = replica_affinity.to_s.to_sym
      @fixed_hostname = fixed_hostname.to_s
      @node_configs = build_node_configs(nodes.dup)
      client_config = client_config.reject { |k, _| IGNORE_GENERIC_CONFIG_KEYS.include?(k) }
      @command_builder = client_config.fetch(:command_builder, ::RedisClient::CommandBuilder)
      @client_config = merge_generic_config(client_config, @node_configs)
      @client_implementation = client_implementation
      @mutex = Mutex.new
    end

    def dup
      self.class.new(
        nodes: @node_configs,
        replica: @replica,
        replica_affinity: @replica_affinity,
        fixed_hostname: @fixed_hostname,
        client_implementation: @client_implementation,
        **@client_config
      )
    end

    def inspect
      "#<#{self.class.name} #{per_node_key.values}>"
    end

    def read_timeout
      @client_config[:read_timeout] || @client_config[:timeout] || RedisClient::Config::DEFAULT_TIMEOUT
    end

    def new_pool(size: 5, timeout: 5, **kwargs)
      @client_implementation.new(self, pool: { size: size, timeout: timeout }, **kwargs)
    end

    def new_client(**kwargs)
      @client_implementation.new(self, **kwargs)
    end

    def per_node_key
      @node_configs.to_h do |config|
        node_key = ::RedisClient::Cluster::NodeKey.build_from_host_port(config[:host], config[:port])
        config = @client_config.merge(config)
        config = config.merge(host: @fixed_hostname) unless @fixed_hostname.empty?
        [node_key, config]
      end
    end

    def use_replica?
      @replica
    end

    def update_node(addrs)
      return if @mutex.locked?

      @mutex.synchronize { @node_configs = build_node_configs(addrs) }
    end

    def add_node(host, port)
      return if @mutex.locked?

      @mutex.synchronize { @node_configs << { host: host, port: port } }
    end

    private

    def build_node_configs(addrs)
      configs = Array[addrs].flatten.filter_map { |addr| parse_node_addr(addr) }
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
      return client_config if node_configs.size.zero?

      cfg = node_configs.first
      MERGE_CONFIG_KEYS.each { |k| client_config[k] = cfg[k] if cfg.key?(k) }
      client_config
    end
  end
end
