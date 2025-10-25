#!/usr/bin/env ruby
# frozen_string_literal: true

# Add lib to load path
$LOAD_PATH.unshift File.expand_path("lib", __dir__)

require "google/cloud/spanner/data"
require "google/cloud/spanner/fields"
require "google/cloud/spanner/convert"
require "benchmark/ips"
require "benchmark/memory"

# Require protobuf files
begin
  require "google/cloud/spanner/v1"
rescue LoadError => e
  puts "Note: Full protobuf loading failed (#{e.message}), using simplified mock..."

  # Create minimal mock structures for benchmarking
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

# Save the optimized version
class Google::Cloud::Spanner::Data
  alias to_h_optimized to_h

  # Old implementation for comparison
  def to_h_old(skip_dup_check: nil)
    raise Google::Cloud::Spanner::DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    keys.zip(to_a(skip_dup_check: skip_dup_check)).to_h
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

# Verify both implementations produce the same result
puts "Verifying implementations produce identical results..."
old_result = data_medium.to_h_old
new_result = data_medium.to_h_optimized
if old_result == new_result
  puts "✓ Results are identical!"
else
  puts "✗ Results differ!"
  puts "Old: #{old_result.inspect}"
  puts "New: #{new_result.inspect}"
  exit 1
end

puts "\n" + "="*80
puts "PERFORMANCE COMPARISON"
puts "="*80

Benchmark.ips do |x|
  x.config(time: 3, warmup: 1)

  x.report("OLD to_h (5 fields)") do
    data_small.to_h_old
  end

  x.report("NEW to_h (5 fields)") do
    data_small.to_h_optimized
  end

  x.report("OLD to_h (10 fields)") do
    data_medium.to_h_old
  end

  x.report("NEW to_h (10 fields)") do
    data_medium.to_h_optimized
  end

  x.report("OLD to_h (20 fields)") do
    data_large.to_h_old
  end

  x.report("NEW to_h (20 fields)") do
    data_large.to_h_optimized
  end

  x.compare!
end

puts "\n" + "="*80
puts "MEMORY COMPARISON"
puts "="*80

Benchmark.memory do |x|
  x.report("OLD to_h (5 fields)") do
    data_small.to_h_old
  end

  x.report("NEW to_h (5 fields)") do
    data_small.to_h_optimized
  end

  x.report("OLD to_h (10 fields)") do
    data_medium.to_h_old
  end

  x.report("NEW to_h (10 fields)") do
    data_medium.to_h_optimized
  end

  x.report("OLD to_h (20 fields)") do
    data_large.to_h_old
  end

  x.report("NEW to_h (20 fields)") do
    data_large.to_h_optimized
  end

  x.compare!
end

puts "\n" + "="*80
puts "SUMMARY"
puts "="*80
puts "The optimized implementation:"
puts "  • Eliminates intermediate array allocations"
puts "  • Builds hash directly instead of using keys.zip(...).to_h"
puts "  • Iterates over @grpc_fields directly"
puts "  • Should show significant reduction in allocations"
puts "="*80
