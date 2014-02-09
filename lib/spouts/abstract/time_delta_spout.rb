require 'red_storm'
require 'thread'

module Abstract
  class TimeConfig
    attr_accessor :delta

    def initialize(delta = 5)
      @delta = delta
    end
  end

  class TimeDeltaSpout < RedStorm::DSL::Spout
    def initialize(config)
      @delta = config.delta
      @thread = nil
      @i = 0
    end

    on_init do
      @q = Queue.new
      @should_continue = true
      last_run_time = Time.now
      next_run_time = last_run_time

      log.info("Initializing Time Bolt at #{last_run_time.to_s} with delta #{@delta.to_s}")

      @thread = Thread.new do
        Thread.current.abort_on_exception = true
        while @should_continue do
          next_run_time = next_run_time + @delta

          @q << [@i.to_s, last_run_time]
          sleep(next_run_time.to_f - last_run_time.to_f)
          last_run_time = next_run_time
          @i += 1
        end
      end
    end

    on_close do
      @should_continue = false
      @thread.join
    end
  end
end