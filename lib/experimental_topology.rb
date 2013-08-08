require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/abstract/redis_pub_sub_spout'
require 'spouts/abstract/time_delta_spout'

module NewsAggregator
  class CustomRedisPubSubSpout < Abstract::RedisPubSubSpout
    output_fields :channel, :message

    def initialize
      config = Abstract::RedisConfig.new
      config.channels = %w{TestChannelOne TestChannelTwo}
      config.patterns = %w{Pattern*}
      super(config)
    end

    on_send(:reliable => true, :ack => true) do
      @q.pop if @q.size > 0
    end

    on_ack do |msg_id|
      log.info("Ack'd message #{msg_id}")
    end
  end

  class CustomTimeSpout < Abstract::TimeDeltaSpout
    output_fields :tick

    def initialize
      config = Abstract::TimeConfig.new(10)
      super(config)
    end

    on_send(:reliable => true, :ack => true) do
      @q.pop if @q.size > 0
    end

    on_ack do |msg_id|
      log.info("Ack'd message #{msg_id}")
    end
  end

  class ExperimentalTopology < RedStorm::DSL::Topology
    spout CustomRedisPubSubSpout

    bolt EchoBolt, :id => 'RedisEchoBolt', :ack => true, :parallelism => 4 do
      source CustomRedisPubSubSpout, :fields => %w{channel}
    end

    spout CustomTimeSpout

    bolt EchoBolt, :id => 'TimeEchoBolt', :ack => true, :parallelism => 8 do
      source CustomTimeSpout, :shuffle
    end

    configure do |env|
      #debug false
      max_task_parallelism 10
    end
  end
end