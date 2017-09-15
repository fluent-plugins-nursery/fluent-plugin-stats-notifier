# encoding: UTF-8
class Fluent::StatsNotifierOutput < Fluent::Output
  Fluent::Plugin.register_output('stats_notifier', self)

  # To support log_level option implemented by Fluentd v0.10.43
  unless method_defined?(:log)
    define_method("log") { $log }
  end

  # Define `router` method of v0.12 to support v0.10 or earlier
  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def initialize
    super
    require 'pathname'
  end

  config_param :target_key, :string
  config_param :interval, :time, :default => 5
  config_param :less_than, :float, :default => nil
  config_param :less_equal, :float, :default => nil
  config_param :greater_than, :float, :default => nil
  config_param :greater_equal, :float, :default => nil
  config_param :stats, :string, :default => "max"
  config_param :compare_with, :string, :default => nil # Obsolete. Use aggregate_stats
  config_param :aggregate_stats, :string, :default => "max" # Work only with aggregate :all
  config_param :tag, :string, :default => nil
  config_param :add_tag_prefix, :string, :default => nil
  config_param :remove_tag_prefix, :string, :default => nil
  config_param :add_tag_suffix, :string, :default => nil
  config_param :remove_tag_suffix, :string, :default => nil
  config_param :aggregate, :string, :default => 'all'
  config_param :store_file, :string, :default => nil

  attr_accessor :counts
  attr_accessor :queues
  attr_accessor :saved_duration
  attr_accessor :saved_at
  attr_accessor :last_checked

  def configure(conf)
    super

    @interval = @interval.to_i

    if @less_than and @less_equal
      raise Fluent::ConfigError, "out_stats_notifier: Only either of `less_than` or `less_equal` can be specified."
    end
    if @greater_than and @greater_equal
      raise Fluent::ConfigError, "out_stats_notifier: Only either of `greater_than` or `greater_equal` can be specified."
    end

    @aggregate_stats = @compare_with if @compare_with # Support old version compatibility
    case @aggregate_stats
    when "sum"
      @aggregate_stats = :sum
    when "max"
      @aggregate_stats = :max
    when "min"
      @aggregate_stats = :min
    when "avg"
      @aggregate_stats = :avg
    else
      raise Fluent::ConfigError, "out_stats_notifier: `aggregate_stats` must be one of `sum`, `max`, `min`, `avg`"
    end

    case @stats
    when "sum"
      @stats = :sum
    when "max"
      @stats = :max
    when "min"
      @stats = :min
    when "avg"
      @stats = :avg
    else
      raise Fluent::ConfigError, "out_stats_notifier: `stats` must be one of `sum`, `max`, `min`, `avg`"
    end

    if @tag.nil? and @add_tag_prefix.nil? and @remove_tag_prefix.nil? and @add_tag_suffix.nil? and @remove_tag_suffix.nil?
      raise Fluent::ConfigError, "out_stats_notifier: No tag option is specified"
    end
    @tag_proc = tag_proc

    case @aggregate
    when 'all'
      raise Fluent::ConfigError, "out_stats_notifier: `tag` must be specified with aggregate all" if @tag.nil?
      @aggregate = :all
    when 'tag'
      # raise Fluent::ConfigError, "out_stats_notifier: `add_tag_prefix` must be specified with aggregate tag" if @add_tag_prefix.nil?
      @aggregate = :tag
    else
      raise Fluent::ConfigError, "out_stats_notifier: aggregate allows tag/all"
    end

    @counts = {}
    @queues = {}
    @mutex = Mutex.new
  end

  def start
    super
    load_status(@store_file, @interval) if @store_file
    @watcher = Thread.new(&method(:watcher))
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
    save_status(@store_file) if @store_file
  end

  # Called when new line comes. This method actually does not emit
  def emit(tag, es, chain)
    key = @target_key

    # enqueus
    count = 0; queues = {}
    es.each do |time,record|
      if record[key]
        queues[key] ||= []
        queues[key] << record[key]
      end
      count += 1
    end

    # thread safe merge
    @counts[tag] ||= 0
    @queues[tag] ||= {}
    @mutex.synchronize do
      if queues[key]
        @queues[tag][key] ||= []
        @queues[tag][key].concat(queues[key])
      end
      @counts[tag] += count
    end

    chain.next
  rescue => e
    log.warn "#{e.class} #{e.message} #{e.backtrace.first}"
  end

  # thread callback
  def watcher
    # instance variable, and public accessable, for test
    @last_checked = Fluent::Engine.now
    # skip the passed time when loading @counts form file
    @last_checked -= @passed_time if @passed_time
    while true
      sleep 0.5
      begin
        if Fluent::Engine.now - @last_checked >= @interval
          now = Fluent::Engine.now
          flush_emit(now - @last_checked)
          @last_checked = now
        end
      rescue => e
        log.warn "#{e.class} #{e.message} #{e.backtrace.first}"
      end
    end
  end

  # This method is the real one to emit
  def flush_emit(step)
    time = Fluent::Engine.now
    counts, queues, @counts, @queues = @counts, @queues, {}, {}

    # Get statistical value among events
    evented_queues = {}
    queues.each do |tag, queue|
      evented_queues[tag] ||= {}
      evented_queues[tag][@target_key] = get_stats(queue[@target_key], @stats) if queue[@target_key]
    end

    if @aggregate == :all
      values = evented_queues.values.map {|queue| queue[@target_key] }.compact
      value = get_stats(values, @aggregate_stats)
      output = generate_output(value) if value
      router.emit(@tag, time, output) if output
    else # aggregate tag
      evented_queues.each do |tag, queue|
        value = queue[@target_key]
        output = generate_output(value) if value
        emit_tag = @tag_proc.call(tag)
        router.emit(emit_tag, time, output) if output
      end
    end
  end

  def get_stats(values, method = :max)
    case method
    when :sum
      stats = values.inject(:+)
    when :max
      stats = values.max
    when :min
      stats = values.min
    when :avg
      stats = values.inject(:+) / values.count unless values.empty?
    end
  end

  def generate_output(value)
    return nil if value == 0 # ignore 0 because standby nodes receive 0 message usually
    return nil if @less_than     and @less_than   <= value
    return nil if @less_equal    and @less_equal  <  value
    return nil if @greater_than  and value <= @greater_than
    return nil if @greater_equal and value <  @greater_equal

    output = {}
    output[@target_key] = value
    output
  end

  def tag_proc
    rstrip = Proc.new {|str, substr| str.chomp(substr) }
    lstrip = Proc.new {|str, substr| str.start_with?(substr) ? str[substr.size..-1] : str }
    tag_prefix = "#{rstrip.call(@add_tag_prefix, '.')}." if @add_tag_prefix
    tag_suffix = ".#{lstrip.call(@add_tag_suffix, '.')}" if @add_tag_suffix
    tag_prefix_match = "#{rstrip.call(@remove_tag_prefix, '.')}." if @remove_tag_prefix
    tag_suffix_match = ".#{lstrip.call(@remove_tag_suffix, '.')}" if @remove_tag_suffix
    tag_fixed = @tag if @tag
    if tag_fixed
      Proc.new {|tag| tag_fixed }
    elsif tag_prefix_match and tag_suffix_match
      Proc.new {|tag| "#{tag_prefix}#{rstrip.call(lstrip.call(tag, tag_prefix_match), tag_suffix_match)}#{tag_suffix}" }
    elsif tag_prefix_match
      Proc.new {|tag| "#{tag_prefix}#{lstrip.call(tag, tag_prefix_match)}#{tag_suffix}" }
    elsif tag_suffix_match
      Proc.new {|tag| "#{tag_prefix}#{rstrip.call(tag, tag_suffix_match)}#{tag_suffix}" }
    else
      Proc.new {|tag| "#{tag_prefix}#{tag}#{tag_suffix}" }
    end
  end

  # Store internal status into a file
  #
  # @param [String] file_path
  def save_status(file_path)
    return unless file_path

    begin
      Pathname.new(file_path).open('wb') do |f|
        @saved_at = Fluent::Engine.now
        @saved_duration = @saved_at - @last_checked
        Marshal.dump({
          :counts           => @counts,
          :queues          => @queues,
          :saved_at         => @saved_at,
          :saved_duration   => @saved_duration,
          :target_key       => @target_key,
        }, f)
      end
    rescue => e
      log.warn "out_stats_notifier: Can't write store_file #{e.class} #{e.message}"
    end
  end

  # Load internal status from a file
  #
  # @param [String] file_path
  # @param [Interger] interval
  def load_status(file_path, interval)
    return unless (f = Pathname.new(file_path)).exist?

    begin
      f.open('rb') do |f|
        stored = Marshal.load(f)
        if stored[:target_key] == @target_key
          if stored[:queues]
            if Fluent::Engine.now <= stored[:saved_at] + interval
              @counts = stored[:counts]
              @queues = stored[:queues]
              @saved_at = stored[:saved_at]
              @saved_duration = stored[:saved_duration]

              # skip the saved duration to continue counting
              @last_checked = Fluent::Engine.now - @saved_duration
            else
              log.warn "out_stats_notifier: stored data is outdated. ignore stored data"
            end
          else
            log.warn "out_stats_notifier: stored data is incompatible. ignore stored data"
          end
        else
          log.warn "out_stats_notifier: configuration param was changed. ignore stored data"
        end
      end
    rescue => e
      log.warn "out_stats_notifier: Can't load store_file #{e.class} #{e.message}"
    end
  end

end
