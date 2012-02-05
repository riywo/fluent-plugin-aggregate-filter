module Fluent

class AggregateFilter < TimeSlicedOutput
  Plugin.register_output('aggregate_filter', self)

#  include Fluent::SetTagKeyMixin
#  config_set_default :include_tag_key, false

#  include Fluent::SetTimeKeyMixin
#  config_set_default :include_time_key, true

  config_set_default :buffer_type, 'memory'
  config_set_default :flush_interval, 1
  config_set_default :time_slice_format, '%Y%m%d-%H%M'
  config_set_default :time_slice_wait, 1

  # config_param :hoge, :string, :default => 'hoge'

  def initialize
    super
    # require 'hogepos'
  end

  def configure(conf)
    super
    # @path = conf['path']
  end

  def start
    super
    # init
  end

  def shutdown
    super
    # destroy
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    if @localtime
      key_time = Time.parse(chunk.key).to_i
    else
      key_time = Time.parse(chunk.key+' UTC').to_i
    end
    key_tag = nil

    aggregate = {}
    chunk.msgpack_each { |tag, time, record|
      key_tag = tag # same tag only...
      record.each_pair { |column, value|
        if aggregate[column] == nil
          aggregate[column] = [value]
        else
          aggregate[column].push(value)
        end
      }
    }
    key_tag = 'aggregated.' + key_tag

    record = {}
    aggregate.each_pair { |column, list|
      list.sort!
      record[column+'_num'] = list.length
      record[column+'_min'] = list.first
      record[column+'_max'] = list.last
      th = (list.length * 0.95).truncate
      record[column+'_95pct'] = list[th-1]
      record[column+'_sum'] = list.inject(0){|sum, i| sum + i }
      record[column+'_avg'] = record[column+'_sum'] / record[column+'_num']
    }

    Engine.emit(key_tag, key_time, record)
    puts [key_tag, Time.at(key_time), record].to_json
  end
end

end
