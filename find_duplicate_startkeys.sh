# Script to find overlapping regions on a hbase table
# You can run this script on both an online and offline table
# 
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main find_overlapping_regions.rb
#
include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "find_overlapping_regions"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE_PATH' % NAME
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size < 1 || ARGV.size > 2
  usage
end

# Get cmdline args.
srcdir = fs.makeQualified(Path.new(java.lang.String.new(ARGV[0])))

# Get table name
tableName = nil
if ARGV.size > 1
  tableName = ARGV[1]
  raise IOError("Not supported yet")
elsif
  # If none provided use dirname
  tableName = srcdir.getName()
end
HTableDescriptor.isLegalTableName(tableName.to_java_bytes)

# Figure locations under hbase.rootdir 
# Move directories into place; be careful not to overwrite.
rootdir = Path.new('/hbase')
tableDir = fs.makeQualified(Path.new(rootdir, tableName))

# Scan the .META. and find overlapping regions
LOG.info("Finding regions of " + tableName + " in .META.")
require 'set'

wanted_table = HTable.new(c, tableName)
keys = wanted_table.getStartEndKeys
start_keys = keys.first.map {|x| String.from_java_bytes x }
end_keys = keys.second .map {|x| String.from_java_bytes x }

found_start_keys = Set.new
found_end_keys = Set.new

start_keys.each do |start_key|
	if found_start_keys.member? start_key
		print "Duplicate start key: %s\n" % start_key
	else
		found_start_keys.add start_key
	end
end
end_keys.each do |end_key|
	if found_end_keys.member? end_key
		print "Duplicate end key: %s\n" % end_key
	else
		found_end_keys.add end_key
	end
end

print "Orphan start regions:\n"
p found_start_keys - found_end_keys

print "Dangling end keys:\n"
p found_end_keys - found_start_keys

