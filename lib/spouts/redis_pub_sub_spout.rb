require 'red_storm'
require 'redis'
require 'thread'
require 'config/spouts/redis_pub_sub_spout'

module RedisPubSubSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple, :channel
    on_send(:reliable => true, :ack => true) { @q.pop unless @q.empty? }

    def initialize()
      @host = CONFIG[:host]
      @port = CONFIG[:port]
      @patterns = CONFIG[:patterns]
      @channels = CONFIG[:channels]

      @i = 0
      @threads = []
    end

    on_init do
      @q = Queue.new
      log.info("Attached to #{@host}:#{@port}")

      @threads << Thread.new do
        Thread.current.abort_on_exception = true
        @patterns.each do |pattern|
          Redis.new(:host => @host, :port => @port).psubscribe(pattern) do |on|
            on.pmessage do |pat, ch, message|
              publish_tuple(ch, {:pattern => pat, :message => message})
            end
          end
        end
      end

      @channels.each do |channel|
        @threads << Thread.new do
          Thread.current.abort_on_exception = true
          Redis.new(:host => @host, :port => @port).subscribe(channel) do |on|
            on.message do |ch, message|
              publish_tuple(ch, {:message => message})
            end
          end
        end
      end
    end

    def publish_tuple(channel, message)
      if self.class.reliable?
        @q << [@i.to_s, message, channel]
        @i += 1
      else
        @q << [message, channel]
      end
    end
  end
end
