# frozen_string_literal: true

class RedisClient
  class Cluster
    module NoopCommandBuilder
      module_function

      def generate(args, _kwargs = nil)
        args
      end
    end
  end
end
