require 'red_storm'
require 'rest-client'
require 'active_support/time'

module VircurexSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple

    on_send(:reliable => true, :ack => true) do
      if @last_fetched < 5.seconds.ago
        response = JSON.load RestClient.get 'https://api.vircurex.com/api/get_info_for_currency.json'
        @last_fetched = Time.now
        ["VBTC", {
         :name       => "VircBTC",
         :last_trade => response["BTC"]["USD"]["last_trade"],
         :best_bid   => response["BTC"]["USD"]["highest_bid"],
         :best_ask   => response["BTC"]["USD"]["lowest_ask"],
         :volume     => response["BTC"]["USD"]["volume"],
         :src_name   => "Vircurex",
         :src        => response} ]
      end
    end

    on_init do
      @last_fetched = 10.minutes.ago
      log.info("Scheduling last_fetched to be at #{@last_fetched.strftime("%I:%M%p")}")
    end

    on_ack do |msg_id|
      log.info("Msg ack received and dismissed #{msg_id}")
    end

    on_fail do |msg_id|
      log.warn("Failed to process #{msg_id}: #{data}")
      @last_fetched = Time.now
    end
  end
end
