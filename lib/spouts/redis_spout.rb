require 'red_storm'
require 'redis'
require 'thread'
require 'config/spouts/redis_spout'

module RedisSpout
  class Spout < RedStorm::DSL::Spout
    on_send { @q.pop if @q.size > 0 }

    on_init do
      @q = Queue.new
      log.info("Attached to #{CONFIG[:host]}:#{CONFIG[:port]}")

      @threads << Thread.new do
        Thread.current.abort_on_exception = true
        redis = Redis.new(:host => CONFIG[:host], :port => CONFIG[:port])
        loop do
          if data = redis.blpop(CONFIG[:queue], 0)
            @q << data[1]
          end
        end
      end
    end
  end
end
