require 'red_storm'
require 'rest-client'
require 'active_support/time'

module CampBXSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple

    on_send(:reliable => true, :ack => true) do
      if @last_fetched < 5.seconds.ago
        response = JSON.load RestClient.get 'http://CampBX.com/api/xticker.php'
        @last_fetched = Time.now
        ["CBTC", {
         :name       => "CampBTC",
         :last_trade => response["Last Trade"],
         :best_bid   => response["Best Bid"],
         :best_ask   => response["Best Ask"],
         :src_name   => "CampBX",
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
