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

  class TimeBasedSpout < RedStorm::DSL::Spout
    on_send { @q.pop if @q.size > 0 }

    def initialize(config)
      @delta = config.delta
      @thread = nil
    end

    on_init do
      @q = Queue.new
      last_run_time = Date.new
      log.info("Initializing Time Bolt at #{last_run_time.inspect} with delta #{@delta}")

      @thread = Thread.new do
        Thread.current.abort_on_exception = true
        loop do
          next_run_time = last_run_time + @delta

          @q << {:now => last_run_time}
          sleep(Date::Delta.diff(next_run_time, last_run_time).in_secs)
        end
      end
    end
  end
end