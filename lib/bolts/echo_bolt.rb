class EchoBolt < RedStorm::DSL::Bolt
  on_receive(:emit => true, :ack => true, :anchor => true) do |tuple|
    log.info(tuple)
    tuple[0]
  end
end