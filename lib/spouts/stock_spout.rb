require 'red_storm'
require 'active_support/time'
require 'yahoofinance'
require 'set'
require 'thread'
require 'config/spouts/stock_spout'

module StockSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple
    on_send(:reliable => true, :ack => true) { @q.pop unless @q.empty? }

    on_init do
      @q = Queue.new
      @last_fetched = {}
      @should_continue = true

      @thread = Thread.new do
        Thread.current.abort_on_exception = true

        while @should_continue do
          stocks_to_check = CONFIG[:symbols].select {|sym| not @last_fetched.has_key?(sym) or @last_fetched[sym] < 1.minutes.ago}
          unless stocks_to_check.empty?
            log.info("Stocks to check: " + stocks_to_check.to_s)
            stocks_to_check.each {|qt| @last_fetched[qt] = Time.now}
              YahooFinance::get_standard_quotes(stocks_to_check) do |qt|
                log.info(qt.inspect)
                if qt.valid?
                  @q << [qt.symbol, {
                       :name       => qt.symbol,
                       :last_trade => qt.lastTrade,
                       :best_bid   => qt.bid,
                       :best_ask   => qt.ask,
                       :volume     => qt.averageDailyVolume,
                       :src_name   => "Yahoo",
                       :src        => qt} ]
                  log.info("DONE: " + qt.inspect)
                end
              end
          end
          sleep(1)
        end
      end
      log.info("READY!")
    end

    on_ack do |msg_id|
      log.info("Stock ack received and dismissed #{msg_id}")
    end

    on_fail do |msg_id|
      log.warn("Failed to process #{msg_id}: #{data}")
      @last_fetched[qt] = 1.minutes.ago
    end

    on_close do
      @should_continue = false
      @thread.join
    end
  end
end
