# Script to find and fix overlapping regions using .META.
#
# It outputs the region names in the format "<tableName>,<startKey>,<encodedName>" suitable for use
# with: org.apache.hadoop.hbase.util.Merge
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main fix_overlapping_regions.rb
#
include Java
import java.lang.Integer
import java.lang.Math
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
import org.apache.hadoop.hbase.util.Merge
import org.apache.hadoop.util.ToolRunner 
import org.apache.hadoop.hbase.util.JenkinsHash
import org.apache.hadoop.fs.FileUtil

# Name of this script
NAME = "find_overlapping_regions_meta"

# Print usage for this script
def usage
  puts 'Usage: %s.rb merge|report FILENAME [TABLENAME]' % NAME
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
if ARGV.size > 3 or ARGV.size < 2
  usage
end

merge = ARGV[0]
filename = ARGV[1]

tableToFix = nil
if ARGV.size == 3
  tableToFix = ARGV[2]
  puts "Merging regions in table: " + tableToFix
else
  puts "Merging regions in all tables!"
end

# Get metatable name
metaName = ".META."

inputFile = File.open(filename)

inputFile.each_line do |line|
  bits = line.split(" ")
  if bits.length == 3
    if bits[0] == "Overlap:" or bits[0] == "Gap:"
      region1 = bits[1]
      region2 = bits[2]
      table1 = bits[1].split(",")[0]
      table2 = bits[2].split(",")[0]
      if table1 == table2 
        if (tableToFix == table1 or !tableToFix)
          puts "Found " + bits[0]
          puts "Regions to merge: " + region1 + " and " + region2
	  # Backup the regions before merging them.
	  region1Hash = Math.abs(JenkinsHash.getInstance().hash(region1.to_java_bytes, region1.length, 0))
 	  region2Hash = Math.abs(JenkinsHash.getInstance().hash(region2.to_java_bytes, region2.length, 0))
	  puts "Region Hashes"
          puts region1Hash
	  puts region2Hash
	  # Check region exists
	  tp = Path.new("/hbase/" + table1)
	  tpb = tp.suffix(".bak")
	  if !fs.exists(tpb)
	    fs.mkdirs(tpb)
	  end
          r1p = tp.suffix("/" + region1Hash.to_s)
	  r1pb = tpb.suffix("/" + region1Hash.to_s)
	  r2p = tp.suffix("/" + region2Hash.to_s)
 	  r2pb = tpb.suffix("/" + region2Hash.to_s)
	  puts "Copying " + r1p.to_s + " to " + r1pb.to_s
	  puts "Copying " + r2p.to_s + " to " + r2pb.to_s
	  # Copy to another folder
	  if fs.exists(r1p) && fs.exists(r2p)
            FileUtil.copy(fs, r1p, fs, r1pb, false, true, c)
	    FileUtil.copy(fs, r2p, fs, r2pb, false, true, c)
            if merge == "merge" 
              puts "Performing merge..."
              status = ToolRunner.run(c, Merge.new(c), [table1, region1, region2].to_java(:string))
	      if status == 0
		puts "Merge completed"
              else
                puts "Merge failed"
              end
            end
          else 
            puts "One of these regions does not exist..."
          end
        end
      else
        puts "Warning: regions found were in different tables: " + region1 + " " + region2
      end
    end
  end
end 
