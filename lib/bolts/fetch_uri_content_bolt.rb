require 'net/http'
require 'uri'

class FetchUriContentBolt < RedStorm::DSL::Bolt
  on_receive(:emit => true, :ack => true, :anchor => true) do |tuple|
    if tuple[0].is_a?(Hash) and tuple[0].key?(:uri)
      uri = URI::parse(tuple[0][:uri])
      res = Net::HTTP.start(uri.host, uri.port) { |http| http.get(uri.path) }
      tuple[0][:header] = res.header
      tuple[0][:body] = res.body
      puts res.body
      log.info(tuple[0])
    end

    tuple[0]
  end
end