module RedisPubSubSpout
  CONFIG = {
      :host => 'localhost',
      :port => 6379,
      :patterns => ['PatternTest*'],
      :channels => ['Channel1', 'Channel2'],
  }
end