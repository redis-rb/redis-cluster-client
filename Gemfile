# frozen_string_literal: true

source 'https://rubygems.org'
gemspec name: 'redis-cluster-client'

gem 'async-redis', platform: :mri
gem 'benchmark'
gem 'benchmark-ips'
gem 'hiredis-client', '~> 0.6'
gem 'irb'
gem 'logger'
gem 'memory_profiler'
gem 'minitest'
gem 'rake'
gem 'rbs', '4.1.0.pre.2' if RUBY_ENGINE == 'jruby' # FIXME: https://github.com/ruby/rdoc/issues/1746
gem 'rubocop'
gem 'rubocop-minitest', require: false
gem 'rubocop-performance', require: false
gem 'rubocop-rake', require: false
gem 'stackprof', platform: :mri
gem 'valkey-rb'
gem 'vernier', platform: :mri if RUBY_ENGINE == 'ruby' && Integer(RUBY_VERSION.split('.').first) > 2
