require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/redis_spout'
require 'spouts/redis_pub_sub_spout'
require 'spouts/abstract/time_delta_spout'

module NewsAggregator
  class CustomTimeSpout < Abstract::TimeDeltaSpout
    output_fields :tick

    def initialize
      super(Abstract::TimeConfig.new(10))
    end

    on_send(:reliable => true, :ack => true) { @q.pop if @q.size > 0 }
    on_ack { |msg_id| log.info("Ack'd message #{msg_id}") }
  end

  class ExperimentalTopology < RedStorm::DSL::Topology
    spout RedisSpout::Spout, :id => 'Queue'

    bolt EchoBolt, :id => 'RedisEchoBolt', :parallelism => 40 do
      source 'Queue', :shuffle
    end

    spout RedisPubSubSpout::Spout, :id => 'PubSub'

    bolt EchoBolt, :id => 'RedisPubSubEchoBolt', :parallelism => 10 do
      source 'PubSub', :shuffle
    end

    #spout CustomTimeSpout

    #bolt EchoBolt, :id => 'TimeEchoBolt', :ack => true, :parallelism => 8 do
    #  source CustomTimeSpout, :shuffle
    #end

    configure do |env|
      debug false
      max_task_parallelism 40
    end
  end
end