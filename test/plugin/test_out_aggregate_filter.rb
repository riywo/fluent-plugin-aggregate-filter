require 'helper'
# require 'time'

class AggregateFilterTest < Test::Unit::TestCase
#  TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  def setup
    Fluent::Test.setup
#    FileUtils.rm_rf(TMP_DIR)
#    FileUtils.mkdir_p(TMP_DIR)
  end

  CONFIG = %[
    utc
  ]
  # CONFIG = %[
  #   path #{TMP_DIR}/out_file_test
  #   compress gz
  #   utc
  # ]

  def create_driver(conf = CONFIG)
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::AggregateFilter).configure(conf)
  end

  def test_configure
    #### set configurations
    d = create_driver %[
      add_prefix agg_test
      percentile 90
    ]
    #### check configurations
    assert_equal 'agg_test', d.instance.add_prefix
    assert_equal 90, d.instance.percentile
  end

  def test_emit
    d = create_driver

    list = [27, 41, 78, 60, 29, 12, 33, 37, 15, 29, 14, 14, 65, 48, 30, 48, 44, 74, 71, 36]
    sort_list = list.sort
    num = sort_list.length
    sum = sort_list.inject(0.0){|sum, i| sum + i.to_f }
    avg = sum / num
    min = sort_list.first
    max = sort_list.last
    pct95 = sort_list[(num * 0.95).truncate - 1]
    var = sort_list.inject(0.0){|sum, i| sum + (i-avg)**2.to_f } / num

    time = Time.parse("2011-01-02 13:14:00 UTC").to_i
    i = 0
    list.each {|value|
      d.emit({"a"=>value}, time + i)
      i += 1
    }
    d.run

    emits = d.emits
    assert_equal 1, emits.length
    assert_equal ["aggregated.test", time,
#      {"a_num"=>num, "a_sum"=>sum, "a_min"=>min, "a_max"=>max, "a_avg"=>avg, "a_pct95"=>pct95, "a_var"=>var}
      {"a" => {"num"=>num, "sum"=>sum, "min"=>min, "max"=>max, "avg"=>avg, "pct95"=>pct95, "var"=>var}}
    ], emits[0]

  end

  def test_write
    d = create_driver

    # time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    # d.emit({"a"=>1}, time)
    # d.emit({"a"=>2}, time)

    # ### FileOutput#write returns path
    # path = d.run
    # expect_path = "#{TMP_DIR}/out_file_test._0.log.gz"
    # assert_equal expect_path, path

    # data = Zlib::GzipReader.open(expect_path) {|f| f.read }
    # assert_equal %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] +
    #                 %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n],
    #              data
  end
end

