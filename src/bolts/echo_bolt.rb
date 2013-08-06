class EchoBolt < RedStorm::DSL::Bolt
  on_receive :emit => false, :ack => true do |tuple|
    log.info(tuple)
    tuple[0]
  end
end