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
#require 'pp'
require 'fileutils'
require 'json'
require 'yaml'


PROGRAM_VERSION       = "1.3.0"

CONFIG_FILE_MARKER    = "="
CONFIG_FILE_SEPARATOR = "|"
CONFIG_FILE_POINTERS  = ["seqfile", "treefile"]

CONTROL_FILE_START    = " * Clarisse: start of dynamically generated options"
CONTROL_FILE_END      = " * Clarisse: end of dynamically generated options"
CONTROL_FILE_ALIGN    = 13

FILE_COMMON_PREFIX    = "_clarisse_"
FILE_CONTROL_SUFFIX   = ".ctl"
FILE_STDOUT_SUFFIX    = ".stdout"
FILE_STDERR_SUFFIX    = ".stderr"

COLUMN_SPACING          = " " * 5
COLUMN_ALIGNMENT_TITLE  = "Alignment"
COLUMN_ITERATION_TITLE  = "Iteration"
COLUMN_ITERATION_WIDTH  = COLUMN_ITERATION_TITLE.length
COLUMN_TIME_TITLE       = "Running time (iteration/alignment)"
COLUMN_TIME_WIDTH       = COLUMN_TIME_TITLE.length

COLUMN_EVENT_TITLE      = "Status"
COLUMN_EVENT_START      = "Started"
COLUMN_EVENT_SUCCESS    = "Finished"
COLUMN_EVENT_FAILURE    = "*Failed"
COLUMN_EVENT_WIDTH      = [
  COLUMN_EVENT_START,
  COLUMN_EVENT_SUCCESS,
  COLUMN_EVENT_FAILURE,
  COLUMN_EVENT_TITLE
].max_by(&:length).length

#class ExecError < StandardError; end
#CODEML_ERRORS = /(Error:|Sequence.*?not found|Species.*?\?)/

### REGEX/FILTERS [[[

#FILE_RESULTS_NAME   = FILE_COMMON_PREFIX + "results.csv"
#NOT_FOUND = "N/F"
#RESULTS_FILE_KEY = "outfile"
#RESULTS_VALID_OPTIONS = [RESULTS_FILE_KEY, "lnL", "omega", "w_ratios"]
#FLOAT     = /[+-]?[0-9.]+/
#LNL       = /(?<=ntime:)\s*(#{FLOAT}).*(?<=np:)\s*(#{FLOAT}).*?(#{FLOAT}).*?#{FLOAT}$/
#OMEGA     = /(#{FLOAT})$/
#W_RATIOS  = /w ratios as labels for TreeView:/
### ]]]


### UTILITY FUNCTIONS [[[

# Each thread will receive a list of directories and execute codeml for each directory/run according to the
# operation mode and options
def process(dirs)
  dirs.each do |dir|
    case @operation_mode
    when :existing
      threaded_puts dir.ljust(@column_width) + "Started".ljust(@column_width), verbose: true

      cmd = "cd #{dir}; codeml #{@options[:existing]}"

      if system("#{cmd} >#{FILE_STDOUT} 2>#{FILE_STDERR}")
        threaded_puts(dir.ljust(@column_width) + "Finished", verbose: true)
      else
        @failed_directories << dir
        threaded_puts(dir.ljust(@column_width) + "*Failed*", verbose: true)
        $stderr.puts "Error: codeml execution failed for directory #{dir}. Command was: '#{cmd}'"
      end


    when :config
      ### The object run_information will hold the paths of the control and result files,
      ### which we will use to execute codeml and extract results to generate next run's
      ### control file
      run_information = {}
      dir_total_time = 0
      @config.each do |run, options|
        begin
          control_file = FILE_COMMON_PREFIX + run.to_s + FILE_CONTROL_SUFFIX
          run_information[run] = {}
          run_information[run][:control] = "#{dir}/#{control_file}"

          File.open("#{dir}/#{control_file}", 'w') do |file|
            file.puts CONTROL_FILE_START

            # For each option, check if it starts with CONFIG_FILE_MARKER so parsing is required
            options.each do |key,value|
              if value.is_a? String and value.start_with? CONFIG_FILE_MARKER
                # File resolution
                # TODO Move this to a function and use it here and when checking options
                if CONFIG_FILE_POINTERS.include? key
                  files_found = Dir.glob("#{dir}/#{value[1..-1]}")
                  if files_found.none?
                    abort "Error: Could not find file #{dir}/#{value[1..-1]}. (#{dir}/#{run})"
                  end
                  dir_path = Pathname.new(dir)
                  value = Pathname.new(files_found.first).relative_path_from(dir_path).to_s

                  
                # Previous result extraction
                else
                  source_run, keyword = value[1..-1].split(CONFIG_FILE_SEPARATOR)
                  if not keyword.is_a? Regexp
                    keyword = /#{keyword}/ 
                  end

                  source_file = run_information[source_run.to_i][:out]
                  value = extract_value_from(keyword, source_file)
                  if value.nil?
                    abort "Error: Extracting #{key} from file #{source_file} failed."
                  end
                end
              end

              file.puts format_option(key, value)
            end

            file.puts CONTROL_FILE_END

            # If a template was specified, append its contents
            if @options[:template]
              File.open(@options[:template]) do |tmpl|
                tmpl.each_line do |line|
                  file.puts line
                end
              end
            end
          end

          # Add the outfile to this run's info
          run_information[run][:out] = "#{dir}/#{extract_value_from(/outfile/, run_information[run][:control])}"


          #if @options[:verbose]
          #  threaded_puts(msg + COLUMN_EVENT_START.ljust(COLUMN_EVENT_WIDTH))
          #end

          msg  = dir.to_s.ljust(COLUMN_ALIGNMENT_WIDTH) + COLUMN_SPACING
          #msg += "#{run} / #{@total_iterations}".ljust(COLUMN_ITERATION_WIDTH) + COLUMN_SPACING
          msg += run.to_s.ljust(COLUMN_ITERATION_WIDTH) + COLUMN_SPACING

          cmd = "cd #{dir}; codeml #{control_file}"
          file_stdout = FILE_COMMON_PREFIX + run.to_s + FILE_STDOUT_SUFFIX
          file_stderr = FILE_COMMON_PREFIX + run.to_s + FILE_STDERR_SUFFIX

          run_start_time = Time::now 
          system(cmd + ">#{file_stdout} 2>#{file_stderr}")

          if $?.exitstatus.eql? 0
            run_time = Time::now - run_start_time
            dir_total_time += run_time

            if @options[:verbose]
              #msg += COLUMN_EVENT_SUCCESS.ljust(COLUMN_EVENT_WIDTH) + COLUMN_SPACING
              msg += "#{human_readable_time(run_time)} / #{human_readable_time(dir_total_time)}"
              threaded_puts(msg)
            end

          else
            @failed_directories << dir
            if @options[:verbose]
              msg += COLUMN_EVENT_FAILURE.ljust(COLUMN_EVENT_WIDTH)
              threaded_puts(msg)
            end
            $stderr.puts "Error: codeml run for #{dir}/#{run} returned an error code."
            break
            #raise ExecError, out.split("\n").grep(CODEML_ERRORS).join("|")
          end
        end
      end
    end
  end
end

def threaded_puts(message)
  @mutex.synchronize {
    puts message
  }
end

##### Extract a value from a results file to be used in a control file
def extract_value_from keyword, source
  if not File.exist? source
    return nil
  end

  extractor = /(?<==)\s*([^\s]*)/

  File.open(source, "r") do |file|
    file.each_line do |line|
      if keyword.match line
        matches = extractor.match(line).captures
        # TODO Act properly when matches.length != 1
        if matches.any?
          return matches.first
        end
      end
    end
  end
  return nil
end

# Pretty-print a key/value pair for use in an iterative control file
def format_option option, value
  return "#{option.rjust(CONTROL_FILE_ALIGN)} = #{value}"
end

def human_readable_time seconds
  seconds = seconds.to_i
  return "#{(seconds/3600).to_s.rjust(2,'0')}:#{((seconds%3600)/60).to_s.rjust(2,'0')}:#{((seconds%3600)%60).to_s.rjust(2,0.to_s)}"
end

# Read a configuration file for clarisse's config operation mode or results extraction.  Since the requirements for both differ slightly, 
# a type must be defined.
def read_configuration_file(filepath, type=:config)
  if not File.exist? filepath
    abort "Error: File #{filepath} does not exist."
  end

  # Try to read and parse the file as YAML
  begin
    config_tree = YAML::load_file(filepath)
  rescue Psych::SyntaxError
    abort "Error: File #{filepath} contains invalid YAML syntax."
  rescue Exception => e
    abort "Unknown error while reading Clarisse configuration file #{filepath}."
    puts e
  end

  # Check the file has a correct general structure and its keys are ordered numbers.
  if not config_tree.is_a? Hash
    abort "Configuration file #{filepath} is not properly structured. See clarisse --help for examples." 
  end

  # Validate all top-level keys are numbers
  if not config_tree.keys.all? {|key| key.is_a? Integer}
    abort "Error: Configuration options in #{filepath} are not grouped by run numbers."
  end

  case type
  when :config
    # If the configuration file is for the configuration mode, make sure keys are sorted. Not necessary for results extraction.
    if not config_tree.keys.first == 1 or not config_tree.keys.each_cons(2).all? {|a,b| b = a + 1}
      abort "Error: Run numbers in file #{filepath} are not continuous." if not run.eql? current
    end

    config.each do |run, options|
      options.each do |key, value|
        # Make sure the value is at least a string or looks like a number.
        if not (value.is_a? String or value.is_a? Numeric)
          abort "Error: Options in configuration file must be strings or numeric values, but on #{filepath} the value of #{run}:#{key} was detected as #{value}."
        end

        # If the key starts with CONFIG_FILE_MARKER, there is more checking to do
        if value.is_a? String and value.start_with? CONFIG_FILE_MARKER
          if value.length.eql? 1
            abort "Error: Option #{filepath}/#{run}/#{key} is missing the configuration string after #{CONFIG_FILE_MARKER}. Check the manual for information." 
          end

          # If the option requires finding a file, verify that one and only one file is found on every directory
          if CONFIG_FILE_POINTERS.include? key
            @dirs.each do |dir|
              files_found = Dir.glob("#{dir}/#{value[1..-1]}")
              if files_found.none?
                abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. File not found with search: #{dir}/#{value[1..-1]}."
              elsif not files_found.one?
                abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. More than one file found with search: #{dir}/#{value[1..-1]}."
              end
            end

          # If the option does not require a file, check that it has the proper format and points to a valid run
          else
            if run == 1
              abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. First run cannot include an extraction option. Check the manual for information."
            end

            if not value.count(CONFIG_FILE_SEPARATOR).eql? 1
              abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. Format is 'RUN#{CONFIG_FILE_SEPARATOR}KEY'. Check the manual for information."
            end

            source_run, key_to_extract = value[1..-1].split(CONFIG_FILE_SEPARATOR)
            if source_run.empty? or key_to_extract.empty?
              abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. Format is 'RUN#{CONFIG_FILE_SEPARATOR}KEY'. Check the manual for information."
            end

            if source_run >= run
              abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. Run to extract result from is not a previous one. Check the manual for information."
            end
          end
        end
      end
    end


  when :results
    config.each do |run, options|
      if not options.include? RESULTS_FILE_KEY
        abort "Error: File #{filepath} contains no #{RESULTS_FILE_KEY} option on run #{run}." 
      end

      options.each do |key, value|
        if not RESULTS_VALID_OPTIONS.include? key
          abort "Error: File #{filepath} contains invalid option #{key}. Check the manual for valid options."
        end

        if not (value.is_a? TrueClass or value.is_a? FalseClass)
          abort "Error: Validation of option #{filepath}/#{run}/#{key} failed. Check the manual for information."
        end
      end
    end
  end

  return config_tree
end
### ]]]



### PARSE COMMAND-LINE ARGUMENTS [[[
# Empty hash where the parsed command line arguments will be stored.
params = {}

# *parser* holds the definition of the command line arguments to be used by the 
# module OptionParser
parser = OptionParser.new do |options|
  options.banner  = "Usage: clarisse OPTIONS DIR..."

  options.on("-e", "--existing FILE", "Run codeml once on every DIR. FILE must already exist inside every DIR and will be used as control file.") do |path|
    params[:existing] = path
  end

  options.on("-c", "--config FILE", "Run codeml once or more on every DIR. FILE will be used to dynamically generate control files.") do |path|
    params[:config] = path
  end

  options.on("--template FILE", "Valid when using --config. Options defined in FILE will be added to every control file.") do |path|
    params[:template] = path
  end

  options.on("--results FILE", "Options defined in FILE will be used for the extraction of results. Can be used independently.") do |path|
    params[:results] = path
  end

  options.on("-t", "--threads N", "Number of threads in which to initially divide the workload.") do |n|
    params[:threads] = n.to_i
  end

  options.on("-v", "Be verbose.") do |n|
    params[:verbose] = true
  end

  options.on("--version", "Print Clarisse's version number.") do |n|
    params[:version] = true
  end

  options.on("-h", "--help", "Print this screen") do
    puts "Clarisse is a script that automates the execution of codeml over a number of directories. It can copy or generate the necessary control files and then use them to execute codeml."
    puts options
    exit
  end
end

# Execute the actual parsing and catch exceptions in case of invalid or incomplete options
begin parser.parse!
rescue OptionParser::InvalidOption, OptionParser::MissingArgument => e
  # There was a problem parsing the options. Print the error and exit
  puts e
  puts parser
  abort
end

# Defaults that will be combined with the command line arguments.
defaults = {
  threads: 1
}

# Merge the parsed command line arguments with the defaults. The given order assures that any
# parsed option overwrites the defaults.
@options = defaults.merge params
### ]]]




### Process received options [[[

# If the version was requested, print it and exit.
if @options[:version]
  puts "Clarisse #{PROGRAM_VERSION}"
  exit 
end


# Validate that at least one operation mode was specified.
if [ @options[:control],
     @options[:existing],
     @options[:config],
     @options[:results] ].none?
  abort "Error: No operation mode was specified."
end

# Validate that only one operating mode was chosen.
if [ @options[:existing],
     @options[:config] ].count {|mode| not mode.nil?} > 1
  abort "Error: More than one of --control, --config or --existing was used."
end

# Validate directories
@dirs = ARGV

if @dirs.empty?
  abort "Error: No list of directories was provided. Use 'clarisse --help' for information."
end

@dirs.uniq!

# Go through every directory name provided and confirm it's valid.
invalid_dirs = []
@dirs.each do |dir|
  if not File.directory? dir
    invalid_dirs << dir
  end
end

if invalid_dirs.any?
  abort "Error: Invalid directories: #{invalid_dirs.join(", ")}"
end
### ]]]




### Initialize [[[
if @options[:existing]
  missing_files = []
  @dirs.each do |dir|
    file = "#{dir}/#{@options[:existing]}"
    if not File.exist? file
      missing_files << file
    end
  end

  if not missing_files.empty?
    $stderr.puts "> ERROR! The following control files could not be found:"
    missing_files.each do |file|
      $stderr.puts "> " + file
    end
    abort
  end
  @operation_mode = :existing

elsif @options[:config]
  @config = read_configuration_file(@options[:config], type: :config)
  @operation_mode = :config
  @total_iterations = @config.keys.last
end

@failed_directories = []
@mutex = Mutex.new
# ]]]




### QUEUES DISTRIBUTION [[[
# Create an array with N empty arrays, where N matches the number of threads given in the options.
# These arrays will act as word queues for every thread
@queues = Array.new(@options[:threads]) { Array.new }

# Distribute the directories into the queues
current = 0
@dirs.each do |dir|
  @queues[current] << dir
  current += 1
  current = 0 if current.eql? @options[:threads]
end

# Remove empty arrays, if any.
@queues.select!{|q| not q.empty?}
### ]]]



### EXECUTION STARTS

### Print headers [[[
# Find the largest string among the directory names and its column header
COLUMN_ALIGNMENT_WIDTH = [
  @dirs.max_by(&:length),
  COLUMN_ALIGNMENT_TITLE 
].max_by(&:length).length 

header  = COLUMN_ALIGNMENT_TITLE.ljust(COLUMN_ALIGNMENT_WIDTH)  + COLUMN_SPACING
header += COLUMN_ITERATION_TITLE.ljust(COLUMN_ITERATION_WIDTH)  + COLUMN_SPACING  if @operation_mode.eql? :config
header += COLUMN_EVENT_TITLE.ljust(COLUMN_EVENT_WIDTH)          + COLUMN_SPACING  if @options[:verbose]
header += COLUMN_TIME_TITLE.ljust(COLUMN_TIME_WIDTH)
puts header
# ]]]

### Start threaded execution [[[
threads = []
@queues.each do |directories_queue|
  threads << Thread.new{process directories_queue}
end

threads.each do |thread|
  thread.join
end
# ]]]

# Summarize failures, if any
if @failed_directories.any?
  puts
  if @failed_directories.length.eql? @dirs.length
    puts "codeml execution failed on every alignment."
  else
    puts "codeml execution failed on #{@failed_directories.length} alignments:"
    @failed_directories.each do |directory|
      puts directory.to_s
    end
  end
end

##### Generate a results file if we were asked to
#if @options[:results]
#  single = @results.length.eql? 1
#
#  File.open(FILE_RESULTS_NAME, "w") do |out|
#    if single
#      out.puts "Directory;ntime;np;log;omega;w_ratios"
#    else
#      out.puts "Directory;Pass;ntime;np;log;omega;w_ratios"
#    end
#
#    @dirs.each do |dir|
#      @results.each do |pass, options|
#        outfile = "#{dir}/#{options["outfile"]}"
#        lnl = []
#        omega = nil
#        w_ratios = []
#
#        begin
#          tree_found = false
#          File.open(outfile, "r") do |file|
#            file.each_line do |line|
#
#              if options["lnL"]
#                if line.match(/^lnL/)
#                  lnl = line.match(LNL)
#                  if lnl
#                    lnl = lnl.captures
#                  end
#                  next
#                end
#              end
#
#              if options["omega"]
#                if line.match(/^omega/)
#                  omega = line.match(OMEGA).to_s
#                  next
#                end
#              end
#
#              if options["w_ratios"]
#                if line.match(W_RATIOS)
#                  tree_found = true
#                elsif tree_found
#                  #w_ratios = line.scan(/(?<=#)[0-9.]+(?=\s)/).uniq
#                  w_ratios = line.scan(/(?<=#)[0-9.]+(?=\s)/).uniq
#                  tree_found = false
#                  next
#                end
#              end
#            end
#          end
#
#          lnl = [NOT_FOUND]*3 if lnl.empty? and options["lnL"]
#          omega = NOT_FOUND if omega.nil? and options["omega"]
#          w_ratios = [NOT_FOUND] if w_ratios.empty? and options["w_ratios"]
#
#          if single
#            out.print "#{dir};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega}"
#            w_ratios.each do |w|
#              out.print ";#{w}"
#            end
#            out.puts
#
#          else
#            out.print "#{dir};#{pass};#{lnl[0]};#{lnl[1]};#{lnl[2]};#{omega}"
#
#            w_ratios.each do |w|
#              out.print ";#{w}"
#            end
#            out.puts
#
#          end
#
#        rescue Errno::ENOENT => e
#          $stderr.puts "ERROR: #{e.message}"
#        end
#      end
#    end
#  end
#  threaded_puts "OK"
#end
