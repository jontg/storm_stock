class EchoBolt < RedStorm::DSL::Bolt
  on_receive :emit => false do |tuple|
    log.info(tuple[0])
  end
end