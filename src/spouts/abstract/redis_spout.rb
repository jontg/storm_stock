require 'red_storm'
require 'redis'
require 'thread'

module Abstract
  class RedisConfig
    attr_accessor :host, :port, :patterns, :channels

    def initialize(host = "localhost", port = 6379, patterns = [], channels = [])
      @host = host
      @port = port
      @patterns = patterns
      @channels = channels
    end
  end

  class RedisSpout < RedStorm::DSL::Spout
    on_send :reliable => true, :emit => true do
      @q.pop if @q.size > 0
    end

    def initialize(redis_config)
      super()
      @host = redis_config.host
      @port = redis_config.port
      @patterns = redis_config.patterns
      @channels = redis_config.channels

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
              @q << [@i, ch, {:pattern => pat, :message => message}]
              @i += 1
            end
          end
        end
      end

      @channels.each do |channel|
        @threads << Thread.new do
          Thread.current.abort_on_exception = true
          Redis.new(:host => @host, :port => @port).subscribe(channel) do |on|
            on.message do |ch, message|
              @q << [@i, ch, {:message => message}]
              @i += 1
            end
          end
        end
      end
    end
  end
end
