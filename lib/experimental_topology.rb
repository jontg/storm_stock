require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/redis_spout'
require 'spouts/abstract/time_delta_spout'

module NewsAggregator
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
    spout RedisSpout::Spout

    bolt EchoBolt, :id => 'RedisEchoBolt', :parallelism => 10 do
      source RedisSpout::Spout, :shuffle
    end

    #spout CustomTimeSpout

    #bolt EchoBolt, :id => 'TimeEchoBolt', :ack => true, :parallelism => 8 do
    #  source CustomTimeSpout, :shuffle
    #end

    configure do |env|
      debug false
      max_task_parallelism 10
    end
  end
end