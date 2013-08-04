require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/abstract/redis_spout'
require 'spouts/abstract/time_based_spout'

module HelloWorld
  class CustomRedisSpout < Abstract::RedisSpout
    output_fields :tuple

    def initialize
      config = Abstract::RedisConfig.new
      config.channels = ["TestChannelOne", "TestChannelTwo"]
      config.patterns = ["Test*"]
      super(config)
    end
  end

  class CustomTimeSpout < Abstract::TimeBasedSpout
    output_fields :tick

    def initialize
      config = Abstract::TimeConfig.new(Date::Delta.new(Date::Delta.dhms_to_delta(0, 0, 0, 0, 0, 1, 0)))
      super(config)
    end
  end

  class ExperimentalTopology < RedStorm::DSL::Topology
    spout CustomRedisSpout

    bolt EchoBolt, :id => "RedisEchoBolt", :parallelism => 3 do
      source CustomRedisSpout, :fields => ["tuple"]
    end

    spout CustomTimeSpout

    bolt EchoBolt, :id => "TimeEchoBolt", :parallelism => 8 do
      source CustomTimeSpout, :fields => ["tick"]
    end

    configure do |env|
      debug false
      max_task_parallelism 4
      #if env == :local
      #  sleep(10)
      #  cluster.shutdown
      #end
    end
  end
end