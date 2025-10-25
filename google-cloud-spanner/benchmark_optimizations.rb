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

# Test different optimization approaches
class Google::Cloud::Spanner::Data
  alias to_h_v1 to_h

  # Optimization V2: Use integer loop instead of each_with_index
  def to_h_v2(skip_dup_check: nil)
    raise DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    result = {}
    count = @grpc_fields.size
    i = 0
    while i < count
      field = @grpc_fields[i]
      key = field.name.empty? ? i : field.name.to_sym
      value = Google::Cloud::Spanner::Convert.grpc_value_to_object(@grpc_values[i], field.type)

      converted_value = case value
                        when Data
                          value.to_h_v2 skip_dup_check: skip_dup_check
                        when Array
                          value.map do |v|
                            v.is_a?(Data) ? v.to_h_v2(skip_dup_check: skip_dup_check) : v
                          end
                        else
                          value
                        end
      result[key] = converted_value
      i += 1
    end
    result
  end

  # Optimization V3: Use if/elsif instead of case
  def to_h_v3(skip_dup_check: nil)
    raise DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    result = {}
    @grpc_fields.each_with_index do |field, i|
      key = field.name.empty? ? i : field.name.to_sym
      value = Google::Cloud::Spanner::Convert.grpc_value_to_object(@grpc_values[i], field.type)

      if value.is_a?(Data)
        result[key] = value.to_h_v3 skip_dup_check: skip_dup_check
      elsif value.is_a?(Array)
        result[key] = value.map do |v|
          v.is_a?(Data) ? v.to_h_v3(skip_dup_check: skip_dup_check) : v
        end
      else
        result[key] = value
      end
    end
    result
  end

  # Optimization V4: Inline everything, avoid intermediate variables
  def to_h_v4(skip_dup_check: nil)
    raise DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    result = {}
    count = @grpc_fields.size
    i = 0
    while i < count
      field = @grpc_fields[i]
      value = Google::Cloud::Spanner::Convert.grpc_value_to_object(@grpc_values[i], field.type)

      result[field.name.empty? ? i : field.name.to_sym] = if value.is_a?(Data)
        value.to_h_v4 skip_dup_check: skip_dup_check
      elsif value.is_a?(Array)
        value.map { |v| v.is_a?(Data) ? v.to_h_v4(skip_dup_check: skip_dup_check) : v }
      else
        value
      end
      i += 1
    end
    result
  end

  # Optimization V5: Cache field name symbols to avoid repeated to_sym calls
  def to_h_v5(skip_dup_check: nil)
    raise DuplicateNameError if !skip_dup_check && fields.duplicate_names?

    result = {}
    @grpc_fields.each_with_index do |field, i|
      # Cache key calculation
      key = if field.name.empty?
              i
            else
              # to_sym is cached by Ruby for the same string, but let's avoid the call
              @field_keys ||= {}
              @field_keys[i] ||= field.name.to_sym
            end

      value = Google::Cloud::Spanner::Convert.grpc_value_to_object(@grpc_values[i], field.type)

      if value.is_a?(Data)
        result[key] = value.to_h_v5 skip_dup_check: skip_dup_check
      elsif value.is_a?(Array)
        result[key] = value.map { |v| v.is_a?(Data) ? v.to_h_v5(skip_dup_check: skip_dup_check) : v }
      else
        result[key] = value
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

# Verify all implementations produce the same result
puts "Verifying all implementations produce identical results..."
v1_result = data_medium.to_h_v1
v2_result = data_medium.to_h_v2
v3_result = data_medium.to_h_v3
v4_result = data_medium.to_h_v4
v5_result = data_medium.to_h_v5

if v1_result == v2_result && v2_result == v3_result && v3_result == v4_result && v4_result == v5_result
  puts "✓ All implementations produce identical results!"
else
  puts "✗ Results differ!"
  puts "V1: #{v1_result.inspect}"
  puts "V2: #{v2_result.inspect}"
  puts "V3: #{v3_result.inspect}"
  puts "V4: #{v4_result.inspect}"
  puts "V5: #{v5_result.inspect}"
  exit 1
end

puts "\n" + "="*80
puts "PERFORMANCE COMPARISON - 10 FIELDS"
puts "="*80

Benchmark.ips do |x|
  x.config(time: 3, warmup: 1)

  x.report("V1: each_with_index + case") { data_medium.to_h_v1 }
  x.report("V2: while loop") { data_medium.to_h_v2 }
  x.report("V3: if/elsif") { data_medium.to_h_v3 }
  x.report("V4: inline + while") { data_medium.to_h_v4 }
  x.report("V5: cached keys") { data_medium.to_h_v5 }

  x.compare!
end

puts "\n" + "="*80
puts "MEMORY COMPARISON - 10 FIELDS"
puts "="*80

Benchmark.memory do |x|
  x.report("V1: each_with_index + case") { data_medium.to_h_v1 }
  x.report("V2: while loop") { data_medium.to_h_v2 }
  x.report("V3: if/elsif") { data_medium.to_h_v3 }
  x.report("V4: inline + while") { data_medium.to_h_v4 }
  x.report("V5: cached keys") { data_medium.to_h_v5 }

  x.compare!
end

puts "\n" + "="*80
puts "PERFORMANCE COMPARISON - 20 FIELDS"
puts "="*80

Benchmark.ips do |x|
  x.config(time: 3, warmup: 1)

  x.report("V1: each_with_index + case") { data_large.to_h_v1 }
  x.report("V2: while loop") { data_large.to_h_v2 }
  x.report("V3: if/elsif") { data_large.to_h_v3 }
  x.report("V4: inline + while") { data_large.to_h_v4 }
  x.report("V5: cached keys") { data_large.to_h_v5 }

  x.compare!
end

puts "\n" + "="*80
puts "OPTIMIZATION SUMMARY"
puts "="*80
puts "V1: Original optimized version (each_with_index + case)"
puts "V2: Use while loop instead of each_with_index"
puts "V3: Use if/elsif instead of case statement"
puts "V4: Inline everything + while loop"
puts "V5: Cache field name symbols"
puts "="*80
