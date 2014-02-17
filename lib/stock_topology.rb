require 'red_storm'
require 'bolts/echo_bolt'
require 'bolts/stock_indexer_bolt'
require 'spouts/stock_spout'
require 'spouts/btc_guild_spout'
require 'spouts/bitminer_spout'

module NewsAggregator
  class StockTopology < RedStorm::DSL::Topology
    spout StockSpout::Spout, :id => 'StockSymbol' do
      set 'topology.sleep.spout.wait.strategy.time.ms', 200
      set 'topology.max.spout.pending', 30
    end

    spout BtcGuildSpout::Spout, :id => 'BTCGuild' do
      set 'topology.sleep.spout.wait.strategy.time.ms', 200
      set 'topology.max.spout.pending', 30
    end

    spout BitminerSpout::Spout, :id => 'Bitminer' do
      set 'topology.sleep.spout.wait.strategy.time.ms', 200
      set 'topology.max.spout.pending', 30
    end

    bolt EchoBolt, :id => 'EchoBolt', :parallelism => 2 do
      source 'StockSymbol', :shuffle
      source 'BTCGuild', :shuffle
      source 'Bitminer', :shuffle
    end

    bolt StockIndexerBolt, :id => 'IndexerBolt' do
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