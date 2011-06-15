# Script find overlapping regions using .META.
#
# It outputs the region names in the format "<tableName>,<startKey>,<encodedName>" suitable for use
# with: org.apache.hadoop.hbase.util.Merge
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main find_overlapping_regions_meta.rb
#
include Java
import java.lang.Integer
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
import org.apache.hadoop.io.WritableComparator

# Name of this script
NAME = "find_overlapping_regions_meta"

# Print usage for this script
def usage
  puts 'Usage: %s.rb' % NAME
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
if ARGV.size > 0
  usage
end

# Get metatable name
metaName = ".META."

LOG.info("Finding overlapping regions and gaps in .META.")
require 'set'

metatable = HTable.new(c, metaName)
scan = Scan.new()
scanner = metatable.getScanner(scan)

oldHRITableName = nil
previousEndKey = ""
previousRegionName = ""
numOverlaps = 0
numGaps = 0
numRegions = 0
while (result = scanner.next())
  numRegions = numRegions + 1
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  bytes = result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
  hri = Writables.getHRegionInfo(bytes)
  hriTableName = hri.getTableDesc().getNameAsString()
  if oldHRITableName
    startKey = String.from_java_bytes hri.getStartKey()
    endKey = String.from_java_bytes hri.getEndKey()
    
    regionName = hri.getRegionNameAsString()
    if hriTableName == oldHRITableName
      # If current start key is ordered before the last end key, these are overlapping!
      if (startKey <=> previousEndKey) < 0 then
        puts "Overlap: " + previousRegionName + " " + regionName
        puts "End keys - previous: " + previousEndKey + ", current: " + endKey 
        numOverlaps = numOverlaps + 1
      end

      # Conversely if it's after then there's a gap of missing keys
      if (startKey <=> previousEndKey) > 0 then
        puts "Gap: " + previousRegionName + " " + regionName
        puts "End keys - previous: " + previousEndKey + ", current: " + endKey
        numGaps = numGaps + 1
      end
    end

    previousEndKey = endKey
    previousRegionName = regionName
  end
  oldHRITableName = hriTableName
end

puts "Number of overlaps found: " + Integer.toString(numOverlaps)
puts "Number of gaps found: " + Integer.toString(numGaps)
puts "Number of regions found in user tables: " + Integer.toString(numRegions)

