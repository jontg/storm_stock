require 'red_storm'
require 'bolts/echo_bolt'
require 'spouts/stock_spout'

module NewsAggregator
  class StockTopology < RedStorm::DSL::Topology
    spout StockSpout::Spout, :id => 'StockSymbol' do
      set "topology.sleep.spout.wait.strategy.time.ms", 10
      set "topology.max.spout.pending", 30
    end

    bolt EchoBolt, :id => 'EchoBolt', :parallelism => 4 do
      source 'StockSymbol', :shuffle
    end

    #spout CustomTimeSpout

    #bolt EchoBolt, :id => 'TimeEchoBolt', :ack => true, :parallelism => 8 do
    #  source CustomTimeSpout, :shuffle
    #end

    configure do |env|
      debug false
      max_task_parallelism 5
    end
  end
end