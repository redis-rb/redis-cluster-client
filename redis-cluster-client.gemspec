# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name                              = 'redis-cluster-client'
  s.summary                           = 'A Redis cluster client for Ruby'
  s.version                           = '0.7.0'
  s.license                           = 'MIT'
  s.homepage                          = 'https://github.com/redis-rb/redis-cluster-client'
  s.authors                           = ['Taishi Kasuga']
  s.email                             = %w[proxy0721@gmail.com]
  s.required_ruby_version             = '>= 2.7.0'
  s.metadata['rubygems_mfa_required'] = 'true'
  s.metadata['allowed_push_host']     = 'https://rubygems.org'
  s.files                             = Dir['lib/**/*.rb']

  s.add_runtime_dependency 'redis-client', '~> 0.12'
end
