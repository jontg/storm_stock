require 'elasticsearch/api'

class StockIndexerBolt < RedStorm::DSL::Bolt
  on_init do
    @es = Elasticsearch::Client.new hosts: CONFIG[:hosts], reload_connections: true
  end

  on_receive(:emit => true, :ack => true, :anchor => true) do |tuple|
    log.info(tuple[0].inspect)
    @es.index index: '', type: '', body: {
        name: tuple[0].name,
        symbol: tuple[0].symbol,
        changePoints: tuple[0].changePoints,
        marketCap: tuple[0].marketCap,
        bid: tuple[0].bid,
        ask: tuple[0].ask
    }

    tuple[0]
  end
end
