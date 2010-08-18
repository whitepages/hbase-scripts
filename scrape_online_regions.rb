#!/usr/bin/ruby1.9
# Find out if any regions for a given table are online on a given list of
# region servers, asking the region servers directly.
#
# Does so via scraping the web interfaces for the region servers because we had
# a situation where every region for the table was listed as offline in .META.
# (see list_regions.rb), but they actually were still online on the regions
# themselves. Possibly related:
#
# http://web.archiveorange.com/archive/v/gMxNAJ9pszuhbGFmgyE9
#
# A complete guess at the cause is duplicate/overlapping regions caused by some
# bug with region split handling - no evidence for this though, just a wild
# guess in the dark, so take with a very heavy pinch of salt.

require 'hpricot'
require 'open-uri'

if ARGV.size < 2
	print "Syntax: #{$0} TABLE host1 [host2 ...]\n"
	exit 1
end

hosts = ARGV
TABLE = hosts.shift

print "Looking for online regions of table '%s'...\n" % TABLE

server_count = 0
region_count = 0

hosts.each do |host|
	doc = Hpricot open 'http://%s:60030/regionserver.jsp' % host
	rows = doc.search '//tr/td[1]/'
	rows.map! { |x| x.to_plain_text }
	tables = rows.select { |x| x =~ /^[^,]+,[^,]+,[^,]+$/ }.map { |x| x[/^[^,]+/] }.select { |x| x == TABLE }
	unless tables.empty?
		print "%s has %d regions for table %s.\n" % [host,tables.size,TABLE]
		region_count += tables.size
		server_count += 1
	end
end
print "Total: %d servers are hosting %d regions for this table.\n" % [server_count, region_count]
