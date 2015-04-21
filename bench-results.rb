#!/usr/bin/env ruby

versions = {}
ARGV.each do |dir|
	Dir.glob(File.join(dir, "*")) do |v|
		if not File.directory? v
			next
		end

		version = File.basename v
		if not versions.include? version
			versions[version] = []
		end

		Dir.glob(File.join(v, "run-*")) do |r|
			File.open File.join(r, "stdout.log"), "r" do |f|
				result = {}
				f.each_line do |l|
					if l =~ /(Sets|Gets)/
						t = $1
						fields = l.gsub(/\s+/m, ' ').strip.split(" ").map { |v| v =~ /^(---|[\d\.]+)$/ ? v.gsub(/---/, '').to_f : v }
						if t == "Sets"
							result[t.downcase] = fields[1]
						elsif t == "Gets"
							result[t.downcase] = {
								"hit" => fields[2],
								"miss" => fields[3],
							}
						end
					end
				end
				versions[version].push result
			end
		end
	end
end
versions.each_pair do |k, v|
	accum = {
		"set" => [],
		"get" => {
			"hit" => [],
			"miss" => [],
		},
	}
	v.each do |r|
		accum["set"].push r["sets"]
		accum["get"]["hit"].push r["gets"]["hit"]
		accum["get"]["miss"].push r["gets"]["miss"]
	end
	versions[k] = accum
end

def median(array)
	sorted = array.sort
	len = sorted.length
	return (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
end
versions.each_pair do |k, v|
	agg = {
		"set" => median(v["set"]),
		"get" => {
			"hit" => median(v["get"]["hit"]),
			"miss" => median(v["get"]["miss"]),
		},
	}
	versions[k] = agg
end

versions.keys.sort { |a, b| a.gsub(/^.*-/, '').to_i <=> b.gsub(/^.*-/, '').to_i }.each do |k|
	v = versions[k]
	printf("%s\tSets\t%f\n", k, v["set"])
	printf("%s\tGets\t%f\t%f\n", k, v["get"]["hit"], v["get"]["miss"])
end
