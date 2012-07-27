require 'redis/namespace'

module Resque
  class RedisBackend
    # Accepts:
    #   1. A 'hostname:port' String
    #   2. A 'hostname:port:db' String (to select the Redis db)
    #   3. A 'hostname:port/namespace' String (to set the Redis namespace)
    #   4. A Redis URL String 'redis://host:port'
    #   5. An instance of `Redis`, `Redis::Client`, `Redis::DistRedis`,
    #      or `Redis::Namespace`.
    def initialize(server)
      case server
      when String
        if server =~ /redis\:\/\//
          redis = Redis.connect(:url => server, :thread_safe => true)
        else
          server, namespace = server.split('/', 2)
          host, port, db = server.split(':')
          redis = Redis.new(:host => host, :port => port,
          :thread_safe => true, :db => db)
        end
        namespace ||= :resque

        @redis = Redis::Namespace.new(namespace, :redis => redis)
      when Redis::Namespace
        @redis = server
      else
        @redis = Redis::Namespace.new(:resque, :redis => server)
      end
    end

    def id
      # support 1.x versions of redis-rb
      if @redis.respond_to?(:server)
        @redis.server
      elsif redis.respond_to?(:nodes) # distributed
        @redis.nodes.map { |n| n.id }.join(', ')
      else
        @redis.client.id
      end
    end
    
    # to have access to redis instance (backward compatibility)
    def client
      @redis
    end

    # Pushes a job onto a queue. Queue name should be a string and the
    # item should be any JSON string (from Ruby object).
    #
    # Returns nothing
    def push(queue, item)
      watch_queue(queue)
      @redis.rpush "queue:#{queue}", item
    end

    # Pops a job off a queue. Queue name should be a string.
    #
    # Returns a JSON string.
    def pop(queue)
      @redis.lpop("queue:#{queue}")
    end

    # Returns an integer representing the size of a queue.
    # Queue name should be a string.
    def size(queue)
      @redis.llen("queue:#{queue}").to_i
    end

    # Returns JSON string with objects
    def peek(queue, start = 0, count = 1)
      list_range("queue:#{queue}", start, count)
    end

    # Returns an array of all known queues as strings.
    def queues
      @redis.smembers(:queues)
    end

    # Given a queue name, completely deletes the queue.
    def remove_queue(queue)
      @redis.srem(:queues, queue.to_s)
      @redis.del("queue:#{queue}")
    end
    
    def keys
      @redis.keys("*").map do |key|
        key.sub("#{redis.namespace}:", '')
      end
    end
    
    # backward compatibility
    def self.connect
      Redis.respond_to?(:connect) ? Redis.connect : "localhost:6379"
    end
    
    
    private
    
    def watch_queue(queue)
      @redis.sadd(:queues, queue.to_s)
    end
    
    # Does the dirty work of fetching a range of items from a Redis list
    # and converting them into Ruby objects.
    def list_range(key, start = 0, count = 1)
      if count == 1
        @redis.lindex(key, start)
      else
        Array(@redis.lrange(key, start, start+count-1))
      end
    end
    
  end
end
