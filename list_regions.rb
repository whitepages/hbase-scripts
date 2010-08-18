# List regions belonging to a table, what server they're on, and their
# online/offline state. Optionally filter by online/offline state.

# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main list_regions.rb

TABLE_NAME=ARGV[0]
WANT=ARGV[1] ? ARGV[1] : 'all'

unless TABLE_NAME && ['all','online','offline'].include?(WANT)
	print "Syntax: #{$0} TABLE [all|online|offline]\n"
	exit 1
end

class AnyFilter
	def initialize
		@options = Array.new
	end
	def add_option &block
		@options.push block
	end
	def accepts? data
		@options.any? { |block| block.call data }
	end
end

filter = AnyFilter.new
if ['all','online'].include? WANT then
	filter.add_option { |region| !region.isOffline }
end
if ['all','offline'].include? WANT then
	filter.add_option { |region| region.isOffline }
end

require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

import java.util.List
import java.util.ArrayList

config = HBaseConfiguration.new
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)
JAVA_TABLE_NAME=TABLE_NAME.to_java_bytes
REGION_INFO_COLUMN = 'info:regioninfo'.to_java_bytes
SERVER_COLUMN = 'info:server'.to_java_bytes

table = HTable.new config, '.META.'.to_java_bytes

scan = Scan.new
scan.addColumn REGION_INFO_COLUMN
scan.addColumn SERVER_COLUMN
scanner = table.getScanner scan

print "Finding %s regions in %s...\n" % [WANT, TABLE_NAME]
while row = scanner.next
	region = Writables.getHRegionInfo row.getValue(REGION_INFO_COLUMN)
	next unless Bytes.equals(region.getTableDesc.getName, JAVA_TABLE_NAME)
	server_bytes = row.getValue(SERVER_COLUMN)
	server = server_bytes ? String.from_java_bytes(server_bytes) : 'no server'

	table =  String.from_java_bytes region.getTableDesc.getName
	if filter.accepts? region
		print "%s is %s (%s)\n"  % [region.getRegionNameAsString, region.isOffline ? 'offline' : 'online', server]
	end
end
scanner.close
print "Finished.\n"
