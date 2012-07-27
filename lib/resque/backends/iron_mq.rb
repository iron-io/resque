require 'iron_mq'

module Resque
  class IronMQBackendProxyCaller
    def initialize(client, prefix)
      @imq_backend = client
      @prefix = prefix
    end

    def method_missing(name, *args, &block)
      full_name = @prefix.to_s + '_' + name.to_s
      if @imq_backend.respond_to?(full_name)
        @imq_backend.send(full_name, *args, &block)
      else
        super
      end
    end
  end
  
  
  class IronMQBackend
    def initialize(opts = { token: nil, project_id: nil })
      @imq_client = IronMQ::Client.new(opts) 
    end
    
    def method_missing(name, *args, &block)
      if args.length == 0
        IronMQBackendProxyCaller.new(self, name)
      else
        super
      end
    end
    
    def id
      "IronMQ backend, so unique!"
    end
    
    def queue_push(queue, item)
      @imq_client.messages.post(item, queue_name: "queue:#{queue}")
    end
    
    def queue_pop(queue)
      messages = @imq_client.messages.get(queue_name: "queue:#{queue}")
      item = messages.body
      messages.delete
      
      item
    end
    
    def queue_size(queue)
      @imq_client.queues.get(name: "queue:#{queue}").size.to_i
    end
    
    def queue_peek(queue, start = 0, count = 1)
      # get all messages from the queue
      messages = @imq_client.messages.get(queue_name: "queue:#{queue}", n: true)
      # filter by asked range
      msgs_asked_range = messages[start..(start+count)] || []
      # get array of JSON strings
      items = msgs_asked_range.map { |m| m.body }
      # remove from the queue
      msgs_asked_range.each { |m| m.delete }
      
      items
    end
    
    def queues_all
      list_for_namespace('queue')
    end
    
    def queues_remove(queue)
      # get all messages from the queue and remove'em
      @imq_client.messages.get(queue_name: "queue:#{queue}", n: true).each { |m| m.delete }
    end
    
    def keys
      @imq_client.queues.list.map { |key| key.name }
    end
    
    def workers_all
      list_for_namespace('worker')
    end
    
    private
    
    def list_for_namespace(namespace)
      @imq_client.queues.list.collect do |queue|
        if queue.name =~ /^#{namespace}:/
          queue.name.sub("#{namespace}:", '')
        end
      end.compact
    end
  end
end
