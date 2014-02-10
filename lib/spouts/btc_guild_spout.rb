require 'red_storm'
require 'rest-client'
require 'active_support/time'
require 'config/spouts/btc_guild_spout'

module BtcGuildSpout
  class Spout < RedStorm::DSL::Spout
    output_fields :tuple

    on_send(:reliable => true, :ack => true) do
      if @last_fetched < 5.minutes.ago
        response = RestClient.get 'https://www.btcguild.com/api.php?api_key='+CONFIG[:api_key]
        @last_fetched = Time.now
        [Time.now, response]
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