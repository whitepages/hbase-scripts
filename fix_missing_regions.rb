# Script adds a table back to a running hbase.
# Currently only works on a copied aside table.
# You cannot parse arbitrary table name.
# 
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main addtable.rb
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
NAME = "fix_missing_regions"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE_DIR [alternate_tablename]' % NAME
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

# Clean mentions of table from .META.
# Scan the .META. and remove all lines that begin with tablename
LOG.info("Finding regions of " + tableName + " in .META.")
require 'set'

wanted_table = HTable.new(c, tableName)
keys = wanted_table.getStartEndKeys
start_keys = keys.first.map {|x| String.from_java_bytes x }.to_set
end_keys = keys.second.map {|x| String.from_java_bytes x }.to_set

missing_regions = (end_keys - start_keys)
if missing_regions.empty? then
	LOG.info "No missing regions."
else
	LOG.info "Missing regions:"
	p missing_regions
end

# Now, walk the table and per region, add an entry
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
LOG.info("Walking " + srcdir.toString() + " to find missing regions")
statuses = fs.listStatus(srcdir)
for status in statuses
  next unless status.isDir()
  next if status.getPath().getName() == "compaction.dir"
  regioninfofile =  Path.new(status.getPath(), ".regioninfo")
  unless fs.exists(regioninfofile)
    next
  end
  is = fs.open(regioninfofile) 
  hri = HRegionInfo.new()
  hri.readFields(is)
  is.close() 
  start_key = String.from_java_bytes hri.getStartKey
  if missing_regions.member? start_key
    region_name = String.from_java_bytes hri.getRegionName()
    path = status.getPath.toString
    end_key = String.from_java_bytes hri.getEndKey
    LOG.info <<EOF
--- FOUND MISSING REGION ---
Name:  #{region_name}
Path:  #{path}
Start: #{start_key}
End:   #{end_key}
-
EOF
 	# TODO: Need to redo table descriptor with passed table name and then recalculate the region encoded names.
  	p = Put.new(hri.getRegionName())
	p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
	metaTable.put(p)
	LOG.info("Added to catalog: " + hri.toString())
  end
end
