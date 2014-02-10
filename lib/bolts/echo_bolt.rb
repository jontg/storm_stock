class EchoBolt < RedStorm::DSL::Bolt
  on_receive(:emit => true, :ack => true, :anchor => true) do |tuple|
    log.info(tuple[0])
    tuple[0]
  end
end