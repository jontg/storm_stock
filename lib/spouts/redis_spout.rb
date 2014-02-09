require 'red_storm'
require 'redis'
require 'set'
require 'thread'
require 'config/spouts/redis_spout'

module RedisSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple
    on_send(:reliable => true, :ack => true) { @q.pop unless @q.empty? }

    on_init do
      @q = Queue.new
      @keys = Set.new(CONFIG[:max_pending].times)
      @should_continue = true

      log.info("Attaching to #{CONFIG[:host]}:#{CONFIG[:port]} with #{CONFIG[:queue]} failing to #{CONFIG[:failed]}")
      @redis = Redis.new(:host => CONFIG[:host], :port => CONFIG[:port])

      @redis.hgetall(CONFIG[:processing]).each_pair do |key, val|
        @redis.hdel(CONFIG[:processing], key)
        @redis.rpush(CONFIG[:queue], val)
      end

      @thread = Thread.new do
        Thread.current.abort_on_exception = true

        while @should_continue do
          if data = @redis.brpop(CONFIG[:queue], :timeout => 1)
            submit_job(data[1])
          elsif data = @redis.spop(CONFIG[:failed])
            submit_job(data)
          end
          until !@keys.empty? or !@should_continue do
            sleep 0.1
          end
        end
      end
    end

    def submit_job(tuple)
      i = @keys.first
      @keys.delete(i)
      @redis.hsetnx(CONFIG[:processing], i, tuple)
      @q << [i, tuple]
    end

    on_ack do |msg_id|
      @redis.hdel(CONFIG[:processing], msg_id)
      log.info("Redis ack received and dismissed #{msg_id}")
      @keys << msg_id
    end

    on_fail do |msg_id|
      if data = @redis.hget(CONFIG[:processing], msg_id) > 0
        @redis.hdel(CONFIG[:processing], msg_id)
        log.warn("Failed to process #{msg_id}: #{data}")
        if CONFIG[:replay_failed] and CONFIG.key?(:failed)
          @redis.sadd(CONFIG[:failed], data)
        end
      end

      @keys << msg_id
    end

    on_close do
      @should_continue = false
      @thread.join
    end
  end
end
