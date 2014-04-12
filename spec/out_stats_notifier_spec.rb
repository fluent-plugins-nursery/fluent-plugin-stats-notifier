# encoding: UTF-8
require_relative 'spec_helper'

class Fluent::Test::OutputTestDriver
  def emit_with_tag(record, time=Time.now, tag = nil)
    @tag = tag if tag
    emit(record, time)
  end
end

describe Fluent::StatsNotifierOutput do
  before { Fluent::Test.setup }
  CONFIG = %[
    target_key 5xx_count
    tag foo
    # compare_with max
  ]
  let(:tag) { 'foo.bar' }
  let(:driver) { Fluent::Test::OutputTestDriver.new(Fluent::StatsNotifierOutput, tag).configure(config) }

  describe 'test configure' do
    describe 'bad configuration' do
      context "nothing" do
        let(:config) { '' }
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end

      context "less_than and less_equal" do
        let(:config) { CONFIG + %[less_than 2 \n less_equal 3] }
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end

      context "greater_than and greater_equal" do
        let(:config) { CONFIG + %[greater_than 2 \n greater_equal 3] }
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end

      context "not tag option is specified" do
        let(:config) { %[target_key 5xx_count] }
        it { expect { driver }.to raise_error(Fluent::ConfigError) }
      end
    end

    describe 'good configuration' do
      context 'required' do
        let(:config) { CONFIG }
        it { expect { driver }.to_not raise_error }
      end
    end
  end

  describe 'test emit' do
    let(:time) { Time.now.to_i }
    let(:messages) do
      [
        {"4xx_count"=>1,"5xx_count"=>6,"reqtime_max"=>6,"reqtime_min"=>1,"reqtime_avg"=>3},
        {"4xx_count"=>2,"5xx_count"=>6,"reqtime_max"=>5,"reqtime_min"=>2,"reqtime_avg"=>2},
        {"4xx_count"=>3,"5xx_count"=>6,"reqtime_max"=>1,"reqtime_min"=>3,"reqtime_avg"=>4},
      ]
    end
    let(:emit) do
      driver.run { messages.each {|message| driver.emit(message, time) } }
      driver.instance.flush_emit(0)
    end
    let(:config) { CONFIG } # 5xx_count, max
    let(:expected) do
      {
        "5xx_count"=>6,
      }
    end

    context "threshold" do
      context 'no threshold' do # should emit
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'greather than' do
        let(:config) { CONFIG + %[greater_than 5] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'not greather than' do
        let(:config) { CONFIG + %[greater_than 6] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'greather than or equal to' do
        let(:config) { CONFIG + %[greater_equal 6] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'not greather than or equal to' do
        let(:config) { CONFIG + %[greater_equal 7] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'less than or equal to' do
        let(:config) { CONFIG + %[less_equal 6] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'not less than or equal to' do
        let(:config) { CONFIG + %[less_equal 5] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'between' do
        let(:config) { CONFIG + %[greater_equal 1 \n less_equal 9] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'not between' do
        let(:config) { CONFIG + %[greater_equal 1 \n less_equal 4] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end
    end

    context 'aggregate' do
      let(:emit) do
        driver.run do
          driver.emit_with_tag({"5xx_count"=>2}, time, 'foo.bar1')
          driver.emit_with_tag({"5xx_count"=>6}, time, 'foo.bar2')
        end
        driver.instance.flush_emit(0)
      end

      context 'all' do
        let(:config) { CONFIG + %[aggregate all \n tag foo \n compare_with sum] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, {"5xx_count"=>8.0})
        end
        it { emit }
      end

      context 'tag' do
        let(:config) { %[target_key 5xx_count \n aggregate tag \n add_tag_prefix add] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("add.foo.bar1", time, {"5xx_count"=>2.0})
          Fluent::Engine.should_receive(:emit).with("add.foo.bar2", time, {"5xx_count"=>6.0})
        end
        it { emit }
      end
    end

    context 'compare_with (obsolete)' do
      let(:emit) do
        driver.run do
          driver.emit_with_tag({"5xx_count"=>2}, time, 'foo.bar1')
          driver.emit_with_tag({"5xx_count"=>6}, time, 'foo.bar2')
        end
        driver.instance.flush_emit(0)
      end

      context 'avg' do
        let(:config) { CONFIG + %[less_equal 4 \n compare_with avg] }
        let(:expected) do
          {
            "5xx_count" => 4.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'sum' do
        let(:config) { CONFIG + %[less_equal 8 \n compare_with sum] }
        let(:expected) do
          {
            "5xx_count" => 8.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'min' do
        let(:config) { CONFIG + %[less_equal 2 \n compare_with min] }
        let(:expected) do
          {
            "5xx_count" => 2.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'max' do
        let(:config) { CONFIG + %[less_equal 6 \n compare_with max] }
        let(:expected) do
          {
            "5xx_count" => 6.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end
    end

    context 'abnormal case (no data)' do
      let(:emit) do
        driver.run do
        end
        driver.instance.flush_emit(0)
      end

      context 'avg' do
        let(:config) { CONFIG + %[less_equal 4 \n compare_with avg] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'sum' do
        let(:config) { CONFIG + %[less_equal 8 \n compare_with sum] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'min' do
        let(:config) { CONFIG + %[less_equal 2 \n compare_with min] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end

      context 'max' do
        let(:config) { CONFIG + %[less_equal 6 \n compare_with max] }
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_not_receive(:emit)
        end
        it { emit }
      end
    end

    context 'aggregate_stats' do
      let(:emit) do
        driver.run do
          driver.emit_with_tag({"5xx_count"=>2}, time, 'foo.bar1')
          driver.emit_with_tag({"5xx_count"=>6}, time, 'foo.bar2')
        end
        driver.instance.flush_emit(0)
      end

      context 'avg' do
        let(:config) { CONFIG + %[less_equal 4 \n aggregate_stats avg] }
        let(:expected) do
          {
            "5xx_count" => 4.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'sum' do
        let(:config) { CONFIG + %[less_equal 8 \n aggregate_stats sum] }
        let(:expected) do
          {
            "5xx_count" => 8.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'min' do
        let(:config) { CONFIG + %[less_equal 2 \n aggregate_stats min] }
        let(:expected) do
          {
            "5xx_count" => 2.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'max' do
        let(:config) { CONFIG + %[less_equal 6 \n aggregate_stats max] }
        let(:expected) do
          {
            "5xx_count" => 6.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end
    end

    context 'stats' do
      let(:emit) do
        driver.run do
          driver.emit_with_tag({"5xx_count"=>2}, time, 'foo.bar1')
          driver.emit_with_tag({"5xx_count"=>6}, time, 'foo.bar1')
        end
        driver.instance.flush_emit(0)
      end

      context 'avg' do
        let(:config) { CONFIG + %[less_equal 4 \n stats avg] }
        let(:expected) do
          {
            "5xx_count" => 4.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'sum' do
        let(:config) { CONFIG + %[less_equal 8 \n stats sum] }
        let(:expected) do
          {
            "5xx_count" => 8.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'min' do
        let(:config) { CONFIG + %[less_equal 2 \n stats min] }
        let(:expected) do
          {
            "5xx_count" => 2.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end

      context 'max' do
        let(:config) { CONFIG + %[less_equal 6 \n stats max] }
        let(:expected) do
          {
            "5xx_count" => 6.0
          }
        end
        before do
          Fluent::Engine.stub(:now).and_return(time)
          Fluent::Engine.should_receive(:emit).with("foo", time, expected)
        end
        it { emit }
      end
    end

    describe "store_file" do
      let(:store_file) do
        dirname = "tmp"
        Dir.mkdir dirname unless Dir.exist? dirname
        filename = "#{dirname}/test.dat"
        File.unlink filename if File.exist? filename
        filename
      end
      let(:config) { CONFIG + %[greater_equal 0 \n store_file #{store_file}] }

      it 'stored_data and loaded_data should equal' do
        driver.run { messages.each {|message| driver.emit(message, time) } }
        driver.instance.shutdown
        stored_counts = driver.instance.counts
        stored_queues = driver.instance.queues
        stored_saved_at = driver.instance.saved_at
        stored_saved_duration = driver.instance.saved_duration
        driver.instance.counts = {}
        driver.instance.queues = {}
        driver.instance.saved_at = nil
        driver.instance.saved_duration = nil

        driver.instance.start
        loaded_counts = driver.instance.counts
        loaded_queues = driver.instance.queues
        loaded_saved_at = driver.instance.saved_at
        loaded_saved_duration = driver.instance.saved_duration

        loaded_counts.should == stored_counts
        loaded_queues.should == stored_queues
        loaded_saved_at.should == stored_saved_at
        loaded_saved_duration.should == stored_saved_duration
      end
    end
  end
end

