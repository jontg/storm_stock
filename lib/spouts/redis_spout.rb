require 'red_storm'
require 'redis'
require 'thread'
require 'config/spouts/redis_spout'

module RedisSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple

    on_send :reliable => true, :ack => true do
      if @q.size > 0
        @outstanding += 1
        @q.pop
      else
        nil
      end
    end

    on_init do
      @q = Queue.new
      @should_continue = true
      @outstanding = 0
      @i = 0

      log.info("Attaching to #{CONFIG[:host]}:#{CONFIG[:port]} with #{CONFIG[:queue]} appending to #{CONFIG[:processing]}")
      @redis = Redis.new(:host => CONFIG[:host], :port => CONFIG[:port])

      @redis.hgetall(CONFIG[:processing]).each_pair do |key, tuple|
        @redis.rpush(CONFIG[:queue], tuple)
        @redis.hdel(CONFIG[:processing], key)
        log.info("Rewinding #{key} -> #{tuple}")
      end

      @thread = Thread.new do
        Thread.current.abort_on_exception = true

        while @should_continue do
          if data = @redis.brpop(CONFIG[:queue], CONFIG[:processing], :timeout => 10)
            if @redis.hsetnx(CONFIG[:processing], @i.to_s, data[1])
              @q << [@i, data[1]]
              @i = (@i + 1) % CONFIG[:max_pending]
            else
              log.warn("Failed to set #{@i} to #{data}")
              @redis.rpush(CONFIG[:queue], data[1])
              sleep 0.1
            end
          end
          until @q.size + @outstanding < CONFIG[:max_pending] or !@should_continue do
            sleep 0.1
          end
        end
      end
    end

    on_ack do |msg_id|
      @redis.hdel(CONFIG[:processing], msg_id.to_s)
      @outstanding -= 1
      log.debug("Redis ack received and dismissed #{msg_id}")
    end

    on_close do
      @should_continue = false
      @thread.join
    end
  end
end
