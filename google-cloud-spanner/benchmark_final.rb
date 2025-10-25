#!/usr/bin/env ruby
# frozen_string_literal: true

# Add lib to load path
$LOAD_PATH.unshift File.expand_path("lib", __dir__)

require "google/cloud/spanner/data"
require "google/cloud/spanner/fields"
require "google/cloud/spanner/convert"
require "benchmark/ips"
require "benchmark/memory"

# Mock structures
begin
  require "google/cloud/spanner/v1"
rescue LoadError
  module Google
    module Cloud
      module Spanner
        module V1
          class Type
            attr_accessor :code, :array_element_type, :struct_type
            def initialize(code: nil, array_element_type: nil, struct_type: nil)
              @code = code
              @array_element_type = array_element_type
              @struct_type = struct_type
            end
          end

          class StructType
            attr_accessor :fields
            def initialize(fields: [])
              @fields = fields
            end

            class Field
              attr_accessor :name, :type
              def initialize(name: "", type: nil)
                @name = name || ""
                @type = type
              end
            end
          end
        end
      end
    end

    module Protobuf
      class Value
        attr_accessor :null_value, :string_value, :number_value, :bool_value, :list_value, :struct_value

        def initialize(null_value: nil, string_value: nil, number_value: nil, bool_value: nil, list_value: nil, struct_value: nil)
          @null_value = null_value
          @string_value = string_value
          @number_value = number_value
          @bool_value = bool_value
          @list_value = list_value
          @struct_value = struct_value
        end

        def kind
          return :null_value if @null_value
          return :string_value if @string_value
          return :number_value if @number_value
          return :bool_value if !@bool_value.nil?
          return :list_value if @list_value
          return :struct_value if @struct_value
          :null_value
        end
      end

      class ListValue
        attr_accessor :values
        def initialize(values: [])
          @values = values || []
        end
      end
    end
  end
end

# Test optimization
class Google::Cloud::Spanner::Data
  alias to_h_current to_h

  # BEST: Optimized version - if/elsif with direct assignment
  def to_h_best(skip_dup_check: nil)
    raise DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    result = {}
    @grpc_fields.each_with_index do |field, i|
      key = field.name.empty? ? i : field.name.to_sym
      value = Google::Cloud::Spanner::Convert.grpc_value_to_object(@grpc_values[i], field.type)

      result[key] = if value.is_a?(Data)
        value.to_h_best skip_dup_check: skip_dup_check
      elsif value.is_a?(Array)
        value.map { |v| v.is_a?(Data) ? v.to_h_best(skip_dup_check: skip_dup_check) : v }
      else
        value
      end
    end
    result
  end
end

# Setup test data
def create_test_data(size: 10)
  fields = (0...size).map do |i|
    Google::Cloud::Spanner::V1::StructType::Field.new(
      name: "field_#{i}",
      type: Google::Cloud::Spanner::V1::Type.new(code: :STRING)
    )
  end

  values = (0...size).map do |i|
    Google::Protobuf::Value.new(string_value: "value_#{i}")
  end

  Google::Cloud::Spanner::Data.from_grpc(values, fields)
end

puts "Setting up test data..."
data_small = create_test_data(size: 5)
data_medium = create_test_data(size: 10)
data_large = create_test_data(size: 20)
data_xl = create_test_data(size: 50)

# Verify both produce same result
current = data_medium.to_h_current
best = data_medium.to_h_best
if current == best
  puts "✓ Results are identical!"
else
  puts "✗ Results differ!"
  exit 1
end

puts "\n" + "="*80
puts "FINAL PERFORMANCE COMPARISON"
puts "="*80

Benchmark.ips do |x|
  x.config(time: 3, warmup: 1)

  x.report("CURRENT (5 fields)") { data_small.to_h_current }
  x.report("BEST    (5 fields)") { data_small.to_h_best }

  x.report("CURRENT (10 fields)") { data_medium.to_h_current }
  x.report("BEST    (10 fields)") { data_medium.to_h_best }

  x.report("CURRENT (20 fields)") { data_large.to_h_current }
  x.report("BEST    (20 fields)") { data_large.to_h_best }

  x.report("CURRENT (50 fields)") { data_xl.to_h_current }
  x.report("BEST    (50 fields)") { data_xl.to_h_best }

  x.compare!
end

puts "\n" + "="*80
puts "FINAL MEMORY COMPARISON"
puts "="*80

Benchmark.memory do |x|
  x.report("CURRENT (10 fields)") { data_medium.to_h_current }
  x.report("BEST    (10 fields)") { data_medium.to_h_best }

  x.report("CURRENT (20 fields)") { data_large.to_h_current }
  x.report("BEST    (20 fields)") { data_large.to_h_best }

  x.report("CURRENT (50 fields)") { data_xl.to_h_current }
  x.report("BEST    (50 fields)") { data_xl.to_h_best }

  x.compare!
end
