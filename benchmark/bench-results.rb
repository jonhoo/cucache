#!/usr/bin/env ruby

aggregators = {
	"median" => lambda do |array|
		len = array.length
		if len == 0
			return 0
		end
		if len == 1
			return array[0]
		end
		sorted = array.sort
		return (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
	end,
	"max" => lambda { |array| array.max },
	"min" => lambda { |array| array.min },
	"mean" => lambda { |array| array.inject{ |sum, el| sum + el }.to_f / array.size }
}

$aggregator = aggregators["median"]
if ARGV.length > 0 and aggregators.include? ARGV[0]
	$aggregator = aggregators[ARGV.shift]
end

versions = {}
ARGV.each do |dir|
	base = ""
	if File.basename(dir) =~ /^(.*)-(\d+)$/
		base = "#{$1}-"
	end
	Dir.glob(File.join(dir, "*")) do |v|
		if not File.directory? v
			next
		end

		version = base + File.basename(v)
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
				if result.include? "gets"
					versions[version].push result
				end
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
			"total" => [],
		},
	}
	v.each do |r|
		accum["set"].push r["sets"]
		accum["get"]["hit"].push r["gets"]["hit"]
		accum["get"]["miss"].push r["gets"]["miss"]
		accum["get"]["total"].push r["gets"]["hit"] + r["gets"]["miss"]
	end

	if accum["set"].length == 0
		versions.delete k
	else
		versions[k] = accum
	end
end

versions.each_pair do |k, v|
	agg = {
		"set" => $aggregator.call(v["set"]),
		"get" => {
			"hit" => $aggregator.call(v["get"]["hit"]),
			"miss" => $aggregator.call(v["get"]["miss"]),
			"total" => $aggregator.call(v["get"]["total"]),
		},
	}
	versions[k] = agg
end

versions.keys.sort { |a, b| a.gsub(/^.*-/, '').to_i <=> b.gsub(/^.*-/, '').to_i }.each do |k|
	v = versions[k]
	printf("%s\tSets\t%f\n", k, v["set"])
	printf("%s\tGets\t%f\t%f\t%f\n", k, v["get"]["hit"], v["get"]["miss"], v["get"]["total"])
end
