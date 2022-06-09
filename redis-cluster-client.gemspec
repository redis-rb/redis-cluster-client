# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name        = 'redis-cluster-client'
  s.version     = '0.0.0'
  s.summary     = 'A Redis cluster client for Ruby'
  s.authors     = ['Taishi Kasuga']
  s.email       = 'proxy0721@gmail.com'
  s.files       = ['lib/redis-cluster-client.rb']
  s.homepage    = 'https://github.com/redis-rb/redis-cluster-client'
  s.license     = 'MIT'

  s.required_ruby_version = '>= 2.7.0'
  spec.add_runtime_dependency 'redis-client'
end
