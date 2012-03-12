module Fluent

class AggregateFilter < TimeSlicedOutput
  Plugin.register_output('aggregate_filter', self)

#  include Fluent::SetTagKeyMixin
#  config_set_default :include_tag_key, false

#  include Fluent::SetTimeKeyMixin
#  config_set_default :include_time_key, true

  config_set_default :buffer_type, 'memory'
#  config_set_default :flush_interval, 1
  config_set_default :time_slice_format, '%Y-%m-%dT%H:%M:00'
  config_set_default :time_slice_wait, 2

  config_param :add_prefix, :string, :default => 'aggregated'
  config_param :percentile, :integer, :default => 95

  def initialize
    super
    # require 'hogepos'
  end

  def configure(conf)
    super
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

    aggregate = {}
    chunk.msgpack_each { |tag, time, record|
      record.each_pair { |column, value|
        aggregate[tag] = {} if aggregate[tag] == nil
        if aggregate[tag][column] == nil
          aggregate[tag][column] = [value]
        else
          aggregate[tag][column].push(value)
        end
      }
    }

    aggregate.each_pair { |tag, hash|
      tag = @add_prefix + '.' + tag
      record = {}
      hash.each_pair { |column, list|
        stat = {}
        list.map! {|x| x.to_i}
        list.sort!
        stat['num'] = list.length
        stat['min'] = list.first
        stat['max'] = list.last
        th = (stat['num'] * @percentile / 100).truncate
        stat['pct' + @percentile.to_s] = list[th-1]
        stat['sum'] = list.inject(0.0){|sum, i| sum + i }
        stat['avg'] = stat['sum'] / stat['num']
        stat['var'] = list.inject(0.0){|sum, i| sum + (i-stat['avg'])**2.to_f } / stat['num']
        record[column] = stat
      }
      Engine.emit(tag, key_time, record)
    }
  end
end

end
