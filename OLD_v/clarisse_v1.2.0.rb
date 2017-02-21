#!/usr/bin/env ruby

# Dr. Mauricio Carrillo-Tripp
# Biomolecular Diversity Laboratory
# tripplab.com
#
# Copyright 2014 Victor Villa

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
require 'json'
require 'yaml'

class FileError < StandardError; end
class RegexError < StandardError; end
class ConfError < StandardError; end
class ExecError < StandardError; end

@mutex = Mutex.new

PROGRAM_VERSION = "1.2.0"

RIGHT_ALIGNMENT = 13
CONTROL_EXT = "control"
CONFIG_MARKER = "="
CONFIG_SEPARATOR = "|"
NOT_FOUND = "N/F"
MIN_COL_WIDTH = 11
OUTPUT_WIDTH = 55
RESULTS_FILE_KEY = "outfile"
RESULTS_VALID_OPTIONS = [RESULTS_FILE_KEY, "lnL", "omega", "w_ratios"]
CODEML_ERRORS = /(Error:|Sequence.*?not found|Species.*?\?)/
ERROR_PREFIX = "*"
LAST_COLUMN_WIDTH = 23
FILE_RESULTS = "results.csv"


FLOAT = /[+-]?[0-9.]+/
LNL = /(?<=ntime:)\s*(#{FLOAT}).*(?<=np:)\s*(#{FLOAT}).*?(#{FLOAT}).*?#{FLOAT}$/
OMEGA = /(#{FLOAT})$/
W_RATIOS = /w ratios as labels for TreeView:/

defaults = {
  threads: 1
}

params = {}
parser = OptionParser.new do |opts|
  opts.banner  = "Usage: clarisse OPTIONS DIR..."

  opts.on("-f", "--control FILE", "Run codeml once on every DIR. FILE will be copied into every DIR and used as control file.") do |path|
    params[:control] = path
  end

  opts.on("-e", "--existing FILE", "Run codeml once on every DIR. FILE must already exist inside every DIR and will be used as control file.") do |path|
    params[:existing] = path
  end

  opts.on("-c", "--config FILE", "Run codeml once or more on every DIR. FILE will be used to dynamically generate control files.") do |path|
    params[:config] = path
  end

  opts.on("--template FILE", "Valid when using --config. Options defined in FILE will be added to every control file.") do |path|
    params[:template] = path
  end

  opts.on("--results FILE", "Options defined in FILE will be used for the extraction of results. Can be used independently.") do |path|
    params[:results] = path
  end

  opts.on("-t", "--threads N", "Number of threads in which to initially divide the workload.") do |n|
    params[:threads] = n.to_i
  end

  opts.on("-v", "Be verbose.") do |n|
    params[:verbose] = true
  end

  opts.on("--version", "Print Clarisse's version number.") do |n|
    params[:version] = true
  end

  opts.on("-h", "--help", "Print this screen") do
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
  puts "Clarisse #{PROGRAM_VERSION}"
  exit 
end

##### Did we receive a list of directories?
@dirs = ARGV
if @dirs.empty?
  abort "ERROR > Did not receive any directory to operate on, nothing to do."
end

##### Check every argument is a valid directory
@dirs.each do |dir|
  abort "ERROR: #{dir} is not a valid directory" if not File.directory? dir
end

##### Determine operation mode
operating_modes = [@options[:control], @options[:existing], @options[:config]]
all_modes = operating_modes + [@options[:results]]

## Check that no more than one operating mode was set
if not operating_modes.none? and not operating_modes.one?
  abort "ERROR > Conflicting parameters, only one of --control, --config or --existing can be used."
end

## Check if at least one operation mode was set
if all_modes.none?
  abort "ERROR > Did not receive any operation mode, nothing to do. Use --control, --existing, --config or --results.  See clarisse --help for options." 
end

def break_down_extraction string
  source_run, detect, extract = string[1..-1].split(CONFIG_SEPARATOR)
  detect = /#{detect}/ if not detect.is_a? Regexp
  extract = /#{extract}/ if extract and not extract.is_a? Regexp
  return source_run.to_i, detect, extract
end

## Check for individual modes and if we received valid paths and files
def validate_configuration config, type=:config
  ## Check the config file is a Hash whose keys are ordered numbers starting in one and sub-hashes' values are strings or numbers
  raise ConfError, "Configuration file is not a Hash." if not config.is_a? Hash

  runs = config.keys
  raise ConfError, "Configuration options must be grouped by run numbers." if not runs.all? {|run| run.is_a? Integer}

  if type.eql? :config
    current = 1
    runs.each do |run|
      raise ConfError, "Run numbers in configuration file must start at 1 and increase without skipping numbers." if not run.eql? current
      current += 1
    end
  end

  config.each do |run, options|
    options.each do |key, value|
      raise ConfError, "#{RESULTS_FILE_KEY} option missing." if type.eql? :results and not options.include? RESULTS_FILE_KEY
      case type
      when :config
        if not (value.is_a? String or value.is_a? Numeric)
          raise ConfError, "Options in configuration file have to be strings or numbers, but the value of #{key} in run #{run} is #{value}."
        end
        if value.is_a? String and value[0].eql? CONFIG_MARKER
          if key.eql? "seqfile" or key.eql? "treefile"
            raise ConfError, "Missing glob after = in #{key} in run #{run}" if value.length.eql? 1

            @dirs.each do |dir|
              results = Dir.glob("#{dir}/#{value[1..-1]}")
              if results.none?
                raise ConfError, "Could not find file #{dir}/#{value[1..-1]}."
              elsif not results.one?
                raise ConfError, "Found more than one file with #{dir}/#{value[1..-1]}."
              end
            end

          else
            raise ConfError, "Option #{key}:#{value} in run #{run} is missing a #{CONFIG_SEPARATOR} character." if value.count("|").eql? 0
            raise ConfError, "Option #{key}:#{value} in run #{run} has too many '#{CONFIG_SEPARATOR}' characters." if value.count("|") > 2
            source_run, detect, extract = value[1..-1].split(CONFIG_SEPARATOR)
            raise ConfError, "Option #{key}:#{value} in run #{run} specifies no run to extract from." if source_run.empty?
            #puts "source_run: #{source_run}, run: #{run}"
            raise ConfError, "Option #{key}:#{value} in run #{run} asks to extract a result from run #{source_run}." if source_run.to_i >= run
            raise ConfError, "Option #{key}:#{value} in run #{run} has an empty detection part." if detect.empty?
          end
        end
      when :results
        raise ConfError if not RESULTS_VALID_OPTIONS.include? key
        if key.eql? RESULTS_FILE_KEY
          raise ConfError, "Value in #{RESULTS_FILE_KEY} can not be blank" if value.empty?
        else
          if not (value.is_a? TrueClass or value.is_a? FalseClass)
            puts "value.class: #{value.class}, value: #{value}"
            raise ConfError, "Values in the configuration file can be only 'true' or 'false', but the value of #{key} in run #{run} is #{value}."
          end
        end
      end
    end
  end
end



def output message, params={}
  defaults = {stderr: false, verbose: false, continuous: false, newline: false}
  output_options = defaults.merge params
  cmd = output_options[:continuous] ? "print" : "puts"
  string = output_options[:continuous] ? message.ljust(OUTPUT_WIDTH) : message

  @mutex.synchronize {
    if output_options[:verbose]
      if @options[:verbose]
        send(cmd, string) 
        puts if output_options[:newline]
      end
    else
      send(cmd, string)
      puts if output_options[:newline]
    end
  }
end


begin
  if @options[:control]
    output "CONTROL mode set"
    output "Looking for file #{@options[:control]}...", verbose: true, continuous: true
    raise FileError, "File '#{@options[:control]}' does not exist" if not File.exist? @options[:control]
    output "OK", verbose: true, newline: true
    @mode = :control

  elsif @options[:existing]
    puts "EXISTING mode set" if @options[:verbose]

    print "Checking if control file '#{@options[:existing]}' exists in all target directories... " if @options[:verbose]
    missing_files = []
    @dirs.each do |dir|
      file = "#{dir}/#{@options[:existing]}"
      if not File.exist? file
        missing_files << file
      end
    end

    if not missing_files.empty?
      puts "FAIL!\n\n" if @options[:verbose]
      $stderr.puts "> ERROR! The following control files could not be found:"
      missing_files.each do |file|
        $stderr.puts "> " + file
      end
      abort
    end

    puts "OK\n\n" if @options[:verbose]
    @mode = :existing

  elsif @options[:config]
    output "CONFIG mode set"
    name = Pathname.new(@options[:config]).basename
    output "Checking if file '#{name}' exists... ", verbose: true, continuous: true
    raise FileError, "File '#{name}' does not exist" if not File.exist? @options[:config]
    output "OK", verbose: true

    ## Check the config file is valid
    @config = YAML::load_file @options[:config]
    output "Checking if its syntax is valid... ", verbose: true, continuous: true
    validate_configuration @config, :config
    output "OK", verbose: true, newline: true

    @mode = :config
    @last_run = @config.keys.last
  end

  if @options[:results]
    output "RESULTS extraction enabled"
    output "Checking if file '#{@options[:results]}' exists... ", verbose: true, continuous: true
    raise FileError, "Could not find file '#{@options[:results]}'." if not File.exist? @options[:results]
    output "OK", verbose: true

    @results = YAML::load_file @options[:results]
    output "Checking if its syntax is valid... ", verbose: true, continuous: true
    validate_configuration @results, :results
    output "OK", verbose: true
    output "", verbose: true if operating_modes.one?
  end

rescue ConfError, FileError => e
  if @options[:verbose]
    puts "Failed: " + e.message + "\n"
  else
    $stderr.puts "> ERROR! " + e.message
  end
  abort
end

##### Put dirs into queues to be processed in threads
queues = Array.new(@options[:threads]) { Array.new }
current = 0
@dirs.each do |dir|
  queues[current] << dir
  current += 1
  current = 0 if current.eql? @options[:threads]
end
queues.select!{|q| not q.empty?}

def human_readable_time seconds
  seconds = seconds.to_i
  return "#{(seconds/3600).to_s.rjust(2,'0')}:#{((seconds%3600)/60).to_s.rjust(2,'0')}:#{((seconds%3600)%60).to_s.rjust(2,0.to_s)}"
end

##### Here we copy/generate control files and execute codeml in the received list
##### of directories
def process dirs
  dirs.each do |dir|
    case @mode
    when :existing, :control
      begin
        output dir.ljust(@column_width) + "Started".ljust(@column_width), verbose: true

        if @mode.eql? :existing
          cmd = "cd #{dir}; codeml #{@options[:existing]}"
        else
          FileUtils.cp @options[:control], dir
          cmd = "cd #{dir}; codeml #{@options[:control]}"
        end

        out = `#{cmd} 2>&1`

        if $?.exitstatus.eql? 0
          output dir.ljust(@column_width) + "Finished", verbose: true
        else
          @clean_run = false
          @failed_directories << dir
          raise ExecError, out.split("\n").grep(CODEML_ERRORS)
        end

      rescue ExecError => e
        if @options[:verbose]
          puts dir.ljust(@column_width) + "* Failed during execution. codeml returned -> " + e.message + "\n"
        else
          $stderr.puts "> Execution error! Directory #{dir}. codeml returned -> " + e.message
        end
        break
      end


    when :config
      ### The object run_information will hold the paths of the control and result files,
      ### which we will use to execute codeml and extract results to generate next run's
      ### control file
      run_information = {}
      run_total_time = 0
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
                if key.eql? "seqfile"
                  results = Dir.glob("#{dir}/#{value[1..-1]}")
                  if results.none?
                    raise ConfError, "Could not find file #{dir}/#{value[1..-1]}."
                  elsif not results.one?
                    raise ConfError, "Found more than one file with #{dir}/#{value[1..-1]}."
                  end
                  value = Pathname.new(results.first).basename.to_s

                else
                  source_run, detect, extract = value[1..-1].split(CONFIG_SEPARATOR)
                  detect = /#{detect}/ if not detect.is_a? Regexp
                  extract = /#{extract}/ if extract and not extract.is_a? Regexp
                  
                  value = extract run_information[source_run.to_i][:out], detect, extract
                end
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

          if @options[:verbose]
            @mutex.synchronize {
              puts dir.ljust(@column_width) + run.to_s.ljust(@column_width) + "Started"
            }
          end

          cmd = "cd #{dir}; codeml #{ctrl_path}"
          start_time = Time::now 
          out = `#{cmd} 2>&1`
          total_time = Time::now - start_time
          run_total_time += total_time

          if $?.exitstatus.eql? 0
            msg = run.eql?(@last_run) ? "Finished last run" : "Finished"
            msg = msg.ljust(LAST_COLUMN_WIDTH) 
            msg += human_readable_time(total_time).ljust(LAST_COLUMN_WIDTH/2)
            msg += human_readable_time(run_total_time) if run.eql?(@last_run)
            output dir.ljust(@column_width) + run.to_s.ljust(@column_width) + msg, verbose: true
          else
            @clean_run = false
            @failed_directories << dir
            raise ExecError, out.split("\n").grep(CODEML_ERRORS).join("|")
          end

        rescue ConfError => e
          @clean_run = false
          @failed_directories << dir

          if @options[:verbose]
            @mutex.synchronize {
              puts dir.ljust(@column_width) + run.to_s.ljust(@column_width) + "* Failed to generate control file. #{e.message}"
            }
          else
            $stderr.puts "> ERROR! Generation of control file failed in run #{run} of directory #{dir}. #{e.message}. Skipping pending runs in this directory."
          end
          break
          
        rescue FileError, RegexError => e
          if @options[:verbose]
            puts "Failed: " + e.message + "\n"
          else
            $stderr.puts "> ERROR! " + e.message
          end
          break
        rescue ExecError => e
          if @options[:verbose]
            puts dir.ljust(@column_width) + run.to_s.ljust(@column_width) + "* Failed during execution. codeml returned '#{e.message}'\n"
          else
            $stderr.puts "> Execution error! Directory #{dir}, Run #{run}. codeml returned '#{e.message}'"
          end
          break
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
@clean_run = true
if operating_modes.one?
  output "EXECUTION started"
  @failed_directories = []

  largest_name = @dirs.max_by(&:length).length
  @column_width = largest_name > MIN_COL_WIDTH ? largest_name : MIN_COL_WIDTH
  @column_width += 2

  if @options[:verbose]
    case @mode
    when :existing, :control
      output "\nDirectory".ljust(@column_width+1) + "Action"
      output "-" * @column_width * 2
    when :config
      output "\nDirectory".ljust(@column_width+1) + "Run".ljust(@column_width) + "Action".ljust(LAST_COLUMN_WIDTH) + "Run time".ljust(LAST_COLUMN_WIDTH/2) + "Total time"
      output "-" * @column_width * 6
    end
  end

  threads = []
  queues.each do |dirs|
    threads << Thread.new{process dirs}
  end
  threads.each do |t|
    t.join
  end

  output "\n", verbose: true
  output "EXECUTION finished"
  output "\n", verbose: true
end

##### Generate a results file if we were asked to
if @options[:results]
  output "Extracting results to file #{FILE_RESULTS}...", continuous: true
  if not @clean_run
    output "#{ERROR_PREFIX}Failed! Skipping results extraction because there were errors during execution."
    abort
  end

  single = @results.length.eql? 1

  File.open(FILE_RESULTS, "w") do |out|
    if single
      out.puts "Directory;ntime;np;log;omega;w_ratios"
    else
      out.puts "Directory;Pass;ntime;np;log;omega;w_ratios"
    end

    @dirs.each do |dir|
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
                  lnl = line.match(LNL)
                  if lnl
                    lnl = lnl.captures
                  end
                  next
                end
              end

              if options["omega"]
                if line.match(/^omega/)
                  omega = line.match(OMEGA).to_s
                  next
                end
              end

              if options["w_ratios"]
                if line.match(W_RATIOS)
                  tree_found = true
                elsif tree_found
                  #w_ratios = line.scan(/(?<=#)[0-9.]+(?=\s)/).uniq
                  w_ratios = line.scan(/(?<=#)[0-9.]+(?=\s)/).uniq
                  tree_found = false
                  next
                end
              end
            end
          end

          lnl = [NOT_FOUND]*3 if lnl.empty? and options["lnL"]
          omega = NOT_FOUND if omega.nil? and options["omega"]
          w_ratios = [NOT_FOUND] if w_ratios.empty? and options["w_ratios"]

          if single
            out.print "#{dir};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega}"
            w_ratios.each do |w|
              out.print ";#{w}"
            end
            out.puts

          else
            out.print "#{dir};#{pass};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega}"

            w_ratios.each do |w|
              out.print ";#{w}"
            end
            out.puts

          end

        rescue Errno::ENOENT => e
          $stderr.puts "ERROR: #{e.message}"
        end
      end
    end
  end
  output "OK"
end

if not @clean_run
  output "During execution, codeml returned an error code while executing in the following directories:\n#{@failed_directories.sort.join(",")}" 
  abort
end

