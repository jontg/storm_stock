require 'config/bolts/stock_indexer_bolt'
require 'elasticsearch'

class StockIndexerBolt < RedStorm::DSL::Bolt
  on_init do
    @es = Elasticsearch::Transport::Client.new hosts: CONFIG[:hosts], reload_connections: true
  end

  on_receive(:emit => true, :ack => true, :anchor => true) do |tuple|
    log.info(tuple[0])
    @es.index index: 'STOCK', type: tuple[0].name, body: tuple[0]

    tuple[0]
  end
end
