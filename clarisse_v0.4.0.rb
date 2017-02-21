#!/usr/bin/env ruby
# Copyright 2014 Victor Villa
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'optparse'
require 'pathname'
require 'pp'
require 'fileutils'
require 'yaml'

class FileError < StandardError; end
class RegexError < StandardError; end

@mutex = Mutex.new

PROGRAM_VERSION = "0.4"

RIGHT_ALIGNMENT = 13
CONTROL_EXT = "control"
CONFIG_MARKER = "="
CONFIG_SEPARATOR = "|"

FLOAT = /[+-]?[0-9.]+/
LNL = /(?<=ntime: )(#{FLOAT}).*(?<=np: )(#{FLOAT}).*?(#{FLOAT}).*?#{FLOAT}$/
OMEGA = /(#{FLOAT})$/
W_RATIOS = /w ratios as labels for TreeView:/

defaults = {
  threads: 1
}

params = {}
parser = OptionParser.new do |opts|
  opts.banner  = "Usage: clarisse OPTIONS DIR..."

  opts.on("--control PATH", "File in PATH will be copied to every DIR and used as control file") do |path|
    params[:control] = path
  end

  opts.on("--existing PATH", "PATH is the name of a file that exists already in every DIR and will be used as control file") do |path|
    params[:existing] = path
  end

  opts.on("--config PATH", "File that will be used to dynamically generate control files inside every DIR") do |path|
    params[:config] = path
  end

  opts.on("--template PATH", "File that will be used as a template with the config file") do |path|
    params[:template] = path
  end

  opts.on("--results PATH", "Configuration file for the extraction of results") do |path|
    params[:results] = path
  end

  opts.on("-t", "--threads N", "Number of threads to use") do |n|
    params[:threads] = n.to_i
  end

  opts.on("-v", "--version", "Display the program version") do |n|
    params[:version] = true
  end

  opts.on("-h", "--help", "Display this screen") do
    puts "Clarisse is a script that automates the execution of codeml over a number of directories. It can copy or generate the necessary control files and then use them to execute codeml."
    puts opts
    exit
  end
end

begin parser.parse!
rescue OptionParser::InvalidOption, OptionParser::MissingArgument => e
  puts e
  puts parser
  abort
end

@options = defaults.merge params

if @options[:version]
  $stdout.puts "Clarisse #{PROGRAM_VERSION}"
  exit 
end

##### Determine operation mode
conflict_msg = "ERROR: Conflicting parameters, only one of --control, --config or --existing can be used."
if @options[:control]
  abort conflict_msg if @options[:config] or @options[:existing]
  abort "ERROR: File '#{@options[:control]}' does not exist" if not File.exist? @options[:control]
  puts "mode <- control"
  @mode = :control
elsif @options[:config]
  if @options[:control] or @options[:existing]
    abort conflict_msg
  end
  @config = YAML::load_file @options[:config]
  @mode = :config
elsif @options[:existing]
  if @options[:control] or @options[:config]
    abort conflict_msg
  end
  @mode = :existing
end

if @options[:results]
  abort "ERROR: File '#{@options[:results]}' does not exist" if not File.exist? @options[:results]
  @results = YAML::load_file @options[:results]
end

if not (@options[:control] or @options[:config] or @options[:existing] or @options[:results])
  puts "Did not receive any operation mode, nothing to do." 
  exit
end


##### Did we receive a list of directories?
dirs = ARGV
if dirs.empty?
  puts "Did not receive any directory to operate on, nothing to do."
  exit
end

##### Check every argument is a valid directory
dirs.each do |dir|
  abort "ERROR: #{dir} is not a valid directory" if not File.directory? dir
end

##### Put dirs into queues to be processed in threads
queues = Array.new(@options[:threads]) { Array.new }
current = 0
dirs.each do |dir|
  queues[current] << dir
  current += 1
  current = 0 if current.eql? @options[:threads]
end
queues.select!{|q| not q.empty?}

##### Here we copy/generate control files and execute codeml in the received list
##### of directories
def process dirs
  dirs.each do |dir|
    case @mode
    when :existing, :control
      begin
        @mutex.synchronize { $stdout.puts "#{dir}:\tstart" }
        if @mode.eql? :existing
          `cd #{dir}; codeml #{@options[:existing]}`
        else
          FileUtils.cp @options[:control], dir
          `cd #{dir}; codeml #{@options[:control]}`
        end
        @mutex.synchronize { $stdout.puts "#{dir}:\t\t\tend" }
      rescue FileError => e
        @mutex.synchronize { $stderr.puts "#{dir}:\t\tERROR #{e.message}." }
        raise
      end

    when :config
      ### The object run_information will hold the paths of the control and result files,
      ### which we will use to execute codeml and extract results to generate next run's
      ### control file
      run_information = {}
      @config.each do |run, values|
        begin
          ctrl_path = "#{run}.#{CONTROL_EXT}"
          run_information[run] = {}
          run_information[run][:control] = "#{dir}/#{ctrl_path}"

          ### We generate the control file that will be used on this run. The file
          ### will be named N.control, where N is the run number
          File.open("#{dir}/#{ctrl_path}", 'w') do |ctrl|
            ctrl.puts " *** START OF DYNAMICALLY ADDED OPTIONS ***\n"
            values.each do |key,value|
              if value.to_s[0].eql? CONFIG_MARKER
                source_run, detect, extract = value[1..-1].split(CONFIG_SEPARATOR)
                detect = /#{detect}/ if not detect.is_a? Regexp
                extract = /#{extract}/ if extract and not extract.is_a? Regexp
                
                value = extract run_information[source_run.to_i][:out], detect, extract
              end
              ctrl.puts format_option key, value
            end
            ctrl.puts " *** END OF DYNAMICALLY ADDED OPTIONS ***\n"

            if @options[:template]
              File.open(@options[:template]) do |tmpl|
                tmpl.each_line do |line|
                  ctrl.puts line
                end
              end
            end
          end

          run_information[run][:out] = "#{dir}/#{extract run_information[run][:control], /outfile/}"
          cmd = "cd #{dir}; codeml #{ctrl_path}"
          @mutex.synchronize { $stdout.puts "#{dir} # #{run}:\tstart" }
          `#{cmd}`
          @mutex.synchronize { $stdout.puts "#{dir} # #{run}:\t\t\tend" }
        rescue FileError, RegexError => e
          @mutex.synchronize { $stderr.puts "#{dir} # #{run}:\t\tERROR #{e.message}." }
          raise
        end
      end
    end
  end
end

##### Extract a value from a results file to be used in a control file
def extract source, detect, extract=nil
  raise FileError, "Could not read file #{source}" if not File.exist? source

  extract ||= /(?<==)\s*([^\s]*)/

  File.open(source, "r") do |f|
    f.each_line do |line|
      if detect.match line
        matches = extract.match(line).captures
        raise RegexError, "Found a line containing #{detect.inspect} but could not extract data using #{extract.inspect}" if matches.length < 1
        raise RegexError, "Found a line containing #{detect.inspect} but could not extract a unique piece of data using #{extract.inspect}" if matches.length > 1
        return matches.first
      end
    end
    raise RegexError, "Could not find a line containing #{detect.inspect}"
  end
end

##### Pretty print a pair of key = value to be used in a dynamically generated control file
def format_option option, value
  return "#{option.rjust(RIGHT_ALIGNMENT)} = #{value}"
end


##### EXECUTION STARTS
### Create a thread for each queue containing directories and execute function *process* on each thread
threads = []
queues.each do |dirs|
  threads << Thread.new{process dirs}
end
threads.each do |t|
  t.join
end

##### Generate a results file if we were asked to
if @options[:results]
  $stdout.puts "Started processing results"
  single = @results.length.eql? 1

  File.open("results.csv", "w") do |out|
    if single
      out.puts "Dir;ntime;np;log;omega;ratios1;ratios2;ratios3;ratios4;ratios5;ratios6"
    else
      out.puts "Dir;Pass;ntime;np;log;omega;ratios1;ratios2;ratios3;ratios4;ratios5;ratios6"
    end

    dirs.each do |dir|
      @results.each do |pass, options|
        outfile = "#{dir}/#{options["outfile"]}"
        lnl = []
        omega = nil
        w_ratios = []

        begin
          tree_found = false
          File.open(outfile, "r") do |file|
            file.each_line do |line|

              if options["lnL"]
                if line.match(/^lnL/)
                  lnl = line.match(LNL).captures
                  break
                end
              end

              if options["omega"]
                if line.match(/^omega/)
                  omega = line.match(OMEGA).to_s
                  break
                end
              end

              if options["w_ratios"]
                if line.match(W_RATIOS)
                  tree_found = true
                elsif tree_found
                  w_ratios = line.scan(/(?<=#)[0-9.]+(?=\s)/).uniq
                  break
                end
              end
            end
          end

          lnl = ["ø"]*3 if lnl.nil? and options["lnL"]
          omega = "ø" if omega.nil? and options["omega"]
          w_ratios = ["ø"]*6 if w_ratios.empty? and options["w_ratios"]

          if single
            out.puts "#{dir};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega};#{w_ratios[0]};#{w_ratios[1]};#{w_ratios[2]};#{w_ratios[3]};#{w_ratios[4]};#{w_ratios[5]};#{w_ratios[6]};#{w_ratios[7]}"
          else
            out.puts "#{dir};#{pass};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega};#{w_ratios[0]};#{w_ratios[1]};#{w_ratios[2]};#{w_ratios[3]};#{w_ratios[4]};#{w_ratios[5]};#{w_ratios[6]};#{w_ratios[7]}"
          end

        rescue Errno::ENOENT => e
          $stderr.puts "ERROR: #{e.message}"
        end
      end
    end
  end
  $stdout.puts "Finished processing results"
end
