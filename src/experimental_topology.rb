require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/abstract/redis_spout'
require 'spouts/abstract/time_delta_spout'

module NewsAggregator
  class CustomRedisSpout < Abstract::RedisSpout
    output_fields :id, :channel, :message

    def initialize
      config = Abstract::RedisConfig.new
      config.channels = ["TestChannelOne", "TestChannelTwo"]
      config.patterns = ["Pattern*"]
      super(config)
    end

    on_ack do |msg_id|
      log.info("Ack'd message #{msg_id}")
    end
  end

  class CustomTimeSpout < Abstract::TimeDeltaSpout
    output_fields :tick

    def initialize
      config = Abstract::TimeConfig.new(Date::Delta.new(Date::Delta.dhms_to_delta(0, 0, 0, 0, 0, 10, 0)))
      super(config)
    end

    on_ack do |msg_id|
      log.info("Ack'd message #{msg_id}")
    end
  end

  class ExperimentalTopology < RedStorm::DSL::Topology
    spout CustomRedisSpout

    bolt EchoBolt, :id => "RedisEchoBolt", :ack => true, :parallelism => 4 do
      source CustomRedisSpout, :fields => ["channel"]
    end

    spout CustomTimeSpout

    bolt EchoBolt, :id => "TimeEchoBolt", :ack => true, :parallelism => 8 do
      source CustomTimeSpout, :shuffle
    end

    configure do |env|
      #debug false
      max_task_parallelism 10
    end
  end
end