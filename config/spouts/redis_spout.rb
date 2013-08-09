module RedisSpout
  CONFIG = {
      :host => 'localhost',
      :port => 6379,
      :queue => 'spout_queue',
      :processing => 'spout_processing',
      :max_pending => 100,
  }
end