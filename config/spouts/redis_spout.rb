module RedisSpout
  CONFIG = {
      :host => 'localhost',
      :port => 6379,
      :queue => 'spout_queue',
      :processing => 'spout_processing',
      :replay_failed => true,
      :failed => 'spout_failed',
      :max_pending => 40,
  }
end