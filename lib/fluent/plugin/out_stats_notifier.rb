# encoding: UTF-8
class Fluent::StatsNotifierOutput < Fluent::Output
  Fluent::Plugin.register_output('stats_notifier', self)

  # To support log_level option implemented by Fluentd v0.10.43
  unless method_defined?(:log)
    define_method("log") { $log }
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
  config_param :compare_with, :string, :default => "max"
  config_param :tag, :string
  config_param :add_tag_prefix, :string, :default => nil
  config_param :remove_tag_prefix, :string, :default => nil
  config_param :aggregate, :string, :default => 'all'
  config_param :store_file, :string, :default => nil

  attr_accessor :counts
  attr_accessor :matches
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

    case @compare_with
    when "sum"
      @compare_with = :sum
    when "max"
      @compare_with = :max
    when "min"
      @compare_with = :min
    when "avg"
      @compare_with = :avg
    else
      raise Fluent::ConfigError, "out_stats_notifier: `compare_with` must be one of `sum`, `max`, `min`, `avg`"
    end

    case @aggregate
    when 'all'
      raise Fluent::ConfigError, "out_stats_notifier: `tag` must be specified with aggregate all" if @tag.nil?
    when 'tag'
      raise Fluent::ConfigError, "out_stats_notifier: `add_tag_prefix` must be specified with aggregate tag" if @add_tag_prefix.nil?
    else
      raise Fluent::ConfigError, "out_stats_notifier: aggregate allows tag/all"
    end

    @tag_prefix = "#{@add_tag_prefix}." if @add_tag_prefix
    @tag_prefix_match = "#{@remove_tag_prefix}." if @remove_tag_prefix
    @tag_proc =
      if @tag_prefix and @tag_prefix_match
        Proc.new {|tag| "#{@tag_prefix}#{lstrip(tag, @tag_prefix_match)}" }
      elsif @tag_prefix_match
        Proc.new {|tag| lstrip(tag, @tag_prefix_match) }
      elsif @tag_prefix
        Proc.new {|tag| "#{@tag_prefix}#{tag}" }
      elsif @tag
        Proc.new {|tag| @tag }
      else
        Proc.new {|tag| tag }
      end

    @counts = {}
    @matches = {}
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

    # stats
    count = 0; matches = {}
    es.each do |time,record|
      if record[key]
        # @todo: make an option for calcuation in the same tag. now only sum is supported
        matches[key] = (matches[key] ? matches[key] + record[key] : record[key])
      end
      count += 1
    end

    # thread safe merge
    @counts[tag] ||= 0
    @matches[tag] ||= {}
    @mutex.synchronize do
      if matches[key]
        # @todo: make an option for calcuation in the same tag. now only sum is supported
        @matches[tag][key] = (@matches[tag][key] ? @matches[tag][key] + matches[key] : matches[key])
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
    counts, matches, @counts, @matches = @counts, @matches, {}, {}

    if @aggregate == 'all'
      values = matches.values.map {|match| match[@target_key] }.compact
      stats = get_stats(values)
      output = generate_output(stats) if stats
      Fluent::Engine.emit(@tag, time, output) if output
    else # aggregate tag
      matches.each do |tag, match|
        values = [match[@target_key]]
        stats = get_stats(values)
        output = generate_output(stats) if stats
        emit_tag = @tag_proc.call(tag)
        Fluent::Engine.emit(emit_tag, time, output) if output
      end
    end
  end

  def get_stats(values)
    case @compare_with
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

  def generate_output(stats)
    return nil if stats == 0 # ignore 0 because standby nodes receive 0 message usually
    return nil if @less_than     and @less_than   <= stats
    return nil if @less_equal    and @less_equal  <  stats
    return nil if @greater_than  and stats <= @greater_than
    return nil if @greater_equal and stats <  @greater_equal

    output = {}
    output[@target_key] = stats
    output
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
          :matches          => @matches,
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

          if Fluent::Engine.now <= stored[:saved_at] + interval
            @counts = stored[:counts]
            @matches = stored[:matches]
            @saved_at = stored[:saved_at]
            @saved_duration = stored[:saved_duration]

            # skip the saved duration to continue counting
            @last_checked = Fluent::Engine.now - @saved_duration
          else
            log.warn "out_stats_notifier: stored data is outdated. ignore stored data"
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
