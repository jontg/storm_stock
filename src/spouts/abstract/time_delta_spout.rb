require 'red_storm'
require 'thread'
require 'date/delta'

module Abstract
  class TimeConfig
    attr_accessor :delta

    def initialize(delta = Date::Delta.new(Date::Delta.dhms_to_delta(0, 0, 0, 0, 0, 5, 0)))
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
      last_run_time = Date.new
      next_run_time = last_run_time

      log.info("Initializing Time Bolt at #{last_run_time.inspect.to_s} with delta #{@delta.to_s}")

      @thread = Thread.new do
        Thread.current.abort_on_exception = true
        while @should_continue do
          next_run_time = next_run_time + @delta

          @q << [@i.to_s, last_run_time.to_date]
          sleep(Date::Delta.diff(next_run_time, last_run_time).in_secs)
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