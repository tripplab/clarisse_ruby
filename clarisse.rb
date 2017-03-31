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


PROGRAM_VERSION       = "2.0.1"

CONFIG_FILE_MARKER    = "="
CONFIG_FILE_SEPARATOR = "->"
CONFIG_FILE_POINTERS  = ["seqfile", "treefile"]

CONTROL_FILE_START    = " *** START OF DYNAMICALLY GENERATED OPTIONS ***"
CONTROL_FILE_END      = " *** END OF DYNAMICALLY GENERATED OPTIONS ***"
CONTROL_FILE_ALIGN    = 13

FILE_COMMON_PREFIX    = "_"
FILE_CONTROL_SUFFIX   = ".ctl"
FILE_STDOUT_SUFFIX    = ".stdout"
FILE_STDERR_SUFFIX    = ".stderr"

COLUMN_SPACING          = " " * 5
COLUMN_ALIGNMENT_TITLE  = "Alignment"
COLUMN_ITERATION_TITLE  = "Iteration"
COLUMN_ITERATION_WIDTH  = COLUMN_ITERATION_TITLE.length
COLUMN_TIME_TITLE       = "Running time (iteration/alignment)"
COLUMN_TIME_WIDTH       = COLUMN_TIME_TITLE.length
COLUMN_QUEUE_TITLE      = "Thread"
COLUMN_QUEUE_WIDTH      = COLUMN_QUEUE_TITLE.length
COLUMN_DIRS_TITLE       = "Alignments"
COLUMN_DIRS_WIDTH       = COLUMN_DIRS_TITLE.length

COLUMN_FAILURE_SOURCE_TITLE = "During"
COLUMN_FAILURE_SOURCE = {
  clarisse: "Clarisse execution",
  codeml:   "Codeml execution"
}
COLUMN_FAILURE_SOURCE_WIDTH = COLUMN_FAILURE_SOURCE.values.max_by(&:length).length
COLUMN_FAILURE_REASON_TITLE = "Reason"
COLUMN_FAILURE_REASON_WIDTH = [COLUMN_FAILURE_REASON_TITLE.length, 20].max

COLUMN_EVENT_TITLE      = "Status"
COLUMN_EVENT_START      = "Started"
COLUMN_EVENT_SUCCESS    = "Finished"
COLUMN_EVENT_FAILURE    = "FAILED"
COLUMN_EVENT_WIDTH      = [
  COLUMN_EVENT_START,
  COLUMN_EVENT_SUCCESS,
  COLUMN_EVENT_FAILURE,
  COLUMN_EVENT_TITLE
].max_by(&:length).length

REGEX_ERROR_DETECTION = /^Error.*/
REGEX_EXTRACTION  = /^#{CONFIG_FILE_MARKER}\s*(?<iter>[[:digit:]]+)\s*#{CONFIG_FILE_SEPARATOR}\s*(?<key>[[:alnum:]]+)\s*$/

# TODO: Add better descriptions
HELP_SHORT = <<-EOF
Clarisse is a script that automates the execution of codeml over a number of directories. It can copy or generate the necessary control files and then use them to execute codeml.
EOF

HELP_LONG = <<-EOF
Control files will be created dynamically...

EXPANSION RULES

EOF

class ExtractionError < StandardError; end
class ExecutionError  < StandardError; end
@failed_directories = []
@mutex = Mutex.new

def clarisse_main
  options, directory_list = parse_arguments()

  configuration = load_configuration(options[:config])

  execution_tree = build_configuration_tree(configuration, directory_list)
  execution_tree = resolve_filenames(execution_tree)

  queues = distribute_workload(execution_tree, options[:threads])

  if options[:preview]
    preview_execution(execution_tree, options, queues)
    exit
  end

  begin_execution(queues, options)
  summarize(options)
  exit
end


def parse_arguments
  ### Define command line options [[[
  arguments = {}
  # *parser* holds the definition of the command line arguments to be used by the module OptionParser
  parser = OptionParser.new do |options|
    options.banner  = "Usage: clarisse OPTIONS DIR..."

    #options.on("-e", "--existing FILE", "Run codeml once on every DIR. FILE must already exist inside every DIR and will be used as control file.") do |path|
    #  arguments[:existing] = path
    #end

    options.on("-c", "--config FILE", "FILE will be parsed to determine the number of iterations and their options on every dir *Mandatory*") do |path|
      arguments[:config] = path
    end

    options.on("--template FILE", "The contents of FILE will be appended to the dynamically generated control file.") do |path|
      arguments[:template] = path
    end

    options.on("-t", "--threads N", "Number of threads in which to initially divide the workload.") do |n|
      arguments[:threads] = n.to_i
    end

    options.on("-p", "--preview", "Don't execute codeml, but parse and validate options and present a preview of what would be executed.") do |n|
      arguments[:preview] = true
    end

    options.on("-d", "--debug") do |n|
      arguments[:debug] = true
    end

    options.on("-v", "Be verbose.") do |n|
      arguments[:verbose] = true
    end

    options.on("-V", "--version", "Print Clarisse's version number.") do |n|
      arguments[:version] = true
    end

    options.on("-h", "--help", "Print this screen") do
      puts HELP_SHORT
      puts
      puts options
      puts
      puts HELP_LONG
      exit
    end
  end
  ### ]]]

  # Execute the actual parsing and catch exceptions in case of invalid or incomplete options
  begin parser.parse!
  rescue OptionParser::InvalidOption, OptionParser::MissingArgument => e
    $stderr.puts "Error: Failed while parsing command line arguments.  Check clarisse --help."
    $stderr.puts e
    abort
  end

  if arguments[:version]
    puts "Clarisse #{PROGRAM_VERSION}"
    exit 
  end

  # Combine default options before validation
  defaults = {
    threads: 1,
    verbose: false
  }
  combined_options = defaults.merge(arguments)

  ### Mandatory arguments [[[
  if not combined_options[:config]
    abort "Error: Configuration file was not specified."
  end

  if combined_options[:threads] < 1
    abort "Error: Invalid number of threads."
  end
  ### ]]]

  ### Verify files and directories exist [[[
  if not File.exist? combined_options[:config]
    abort "Error: File not found: #{combined_options[:config]}."
  elsif not File.readable? combined_options[:config]
    abort "Error: Failed to open file: #{combined_options[:config]}."
  end

  if combined_options[:template]
    if not File.exist? combined_options[:template]
      abort "Error: File not found: #{combined_options[:template]}."
    elsif not File.readable? combined_options[:template]
      abort "Error: Failed to open file: #{combined_options[:template]}."
    end
  end


  directory_list = ARGV
  if directory_list.empty?
    abort "Error: No list of directories was provided."
  end

  # Use the cleanpath method to get rid of useless characters, like a tailing slash
  directory_list.map! {|directory| Pathname.new(directory).cleanpath.to_s}
  # Remove duplicates
  directory_list.uniq!

  # Go through every directory name provided and confirm it's valid.
  invalid_dirs = []
  directory_list.each do |dir|
    if not File.directory? dir
      invalid_dirs << dir
    end
  end

  if invalid_dirs.any?
    $stderr.puts "Error: Invalid directories:"
    invalid_dirs.each do |invalid_dir|
      $stderr.puts invalid_dir
    end
  end
  ### ]]]

  # Find the largest string among the directory names and its column header
  @column_alignment_width = [
    directory_list.max_by(&:length),
    COLUMN_ALIGNMENT_TITLE 
  ].max_by(&:length).length 

  return combined_options, directory_list
end


def load_configuration(configuration_file)
  if not File.exist? configuration_file
    abort "Error: File #{configuration_file} does not exist."
  end

  # Try to read and parse the file as YAML
  begin
    iterations = YAML::load_file(configuration_file)
  rescue Psych::SyntaxError => e
    abort "Error: #{configuration_file} contains invalid YAML syntax."
    puts e
  rescue Exception => e
    abort "Error: Unknown problem while reading file #{configuration_file}."
    puts e
  end

  ### Validate general structure [[[
  if not iterations.is_a? Hash
    abort "Error: #{configuration_file} is not properly structured. See the " \
          "documentation for help." 
  end

  if not iterations.keys.all? {|key| key.is_a? Integer}
    abort "Error: #{configuration_file}: iterations options are not grouped " \
          "by iteration number."
  end

  if not iterations.keys.first == 1
    abort "Error: #{configuration_file}: iteration numbers don't start from 1."
  end

  if not iterations.keys.each_cons(2).all? {|a,b| b = a + 1}
    abort "Error: In file #{configuration_file}, iteration numbers must be "  \
          "continuous."
  end
  ### ]]]

  ### Validate syntax for every option [[[
  iterations.each do |iteration, options|
    options.each do |key, value|
      # Exit if there are strange options
      # TODO: Validate there are no empty values?
      # TODO: Restrict the valid characters strings to a limited set
      if not (value.is_a? String or value.is_a? Numeric)
        abort "Error: #{configuration_file}|#{iteration}:#{key}: Invalid value"
      end


      # If the key starts with CONFIG_FILE_MARKER, there is more checking to do
      if value.is_a? String and value.start_with? CONFIG_FILE_MARKER
        # Validate option is not empty
        if value.length.eql? 1
          abort "Error: #{configuration_file}|#{iteration}/#{key}: missing "  \
                "value after #{CONFIG_FILE_MARKER}." 
        end

        # If the option is not a file glob, it must be an extractor for a 
        # previous iteration.  In that case, it must have the '=N->key' format,
        # where N is the iteration number and key is the value to extract
        if not CONFIG_FILE_POINTERS.include? key
          if iteration == 1
            abort "Error: #{configuration_file}|#{iteration}/#{key}: "        \
                  "First iteration cannot include an extraction option."
          end

          if not REGEX_EXTRACTION.match(value)
            abort "Error: Invalid option in configuration file: #{value}."
          end

          source_iteration = REGEX_EXTRACTION.match(value)[:iter].to_i
          if not source_iteration < iteration
            abort "Error: Invalid option in configuration file: "             \
                  "Iteration #{iteration} can't extract a result from "       \
                  "iteration #{source_iteration}."
          end
        end
      end
    end
  end
  ### ]]]

  return iterations
end


def build_configuration_tree(configuration, directory_list)
  iterations_tree = {}
  directory_list.each do |directory|
    iterations_tree[directory] = configuration
  end
  return iterations_tree
end


def resolve_filenames(execution_tree)
  execution_tree.each do |directory, iterations|
    iterations.each do |iteration, options|
      options.each do |key, value|
        if CONFIG_FILE_POINTERS.include? key and value.start_with? CONFIG_FILE_MARKER
          value = value[1..-1].lstrip
          if Pathname.new(value).absolute?
            files_found = Dir.glob(value)
          else
            files_found = Dir.glob(directory + "/" + value)
          end
          if files_found.none?
            abort "Error: #{directory}/#{iteration}/#{key} = #{directory}/#{value}: File not found"
          elsif not files_found.one?
            abort "Error: #{directory}/#{iteration}/#{key} = #{directory}/#{value}: More than one file found"
          else
            relative_path = Pathname.new(files_found.first).relative_path_from(Pathname.new(directory)).to_s
            execution_tree[directory][iteration][key] = relative_path
          end
        end
      end
    end
  end
  return execution_tree
end


def preview_execution(execution_tree, options, queues)
  puts "### Codeml will be executed on #{execution_tree.length} alignments, with #{execution_tree.first.length} iterations each."
  puts "### The following options have been defined for each iteration and all supported file expansions have been resolved in relation to each alignment's directory:"
  puts
  execution_tree.each do |directory, iterations|
    iterations.each do |iteration, options|
      puts "# Alignment: #{directory} / Iteration: #{iteration}:"
      options.each do |key, value|
        if value.is_a? String and value.start_with? CONFIG_FILE_MARKER
          source_iteration = REGEX_EXTRACTION.match(value)[:iter]
          value = "[extract from results of iteration #{source_iteration}]"
        end
        puts "#{key.to_s.rjust(CONTROL_FILE_ALIGN)} = #{value}"
      end
      puts
    end
  end

  if options[:template]
    puts
    puts  "### In addition to the above options, the following template will be appended to each control file."
    puts
    begin
      File.open(options[:template], "r") do |template|
        template.each_line do |line|
          puts line
        end
      end
    rescue Exception => e
      abort "Error: Failed to read template file #{options[:template]}"
    end
    puts
  end

  if options[:threads] > 1
    puts
    puts  "### Execution will be distributed among #{options[:threads]} threads, with the following per-thread workload:"
    puts

    header  = COLUMN_QUEUE_TITLE.ljust(COLUMN_QUEUE_WIDTH) + COLUMN_SPACING
    header += COLUMN_DIRS_TITLE
    puts header

    queue_number = 1
    queues.each_index do |queue|
      queue_printed = false
      queues[queue].keys.each do |directory|
        if not queue_printed
          line  = queue_number.to_s.center(COLUMN_QUEUE_WIDTH) + COLUMN_SPACING
          line += directory.to_s
          puts line
          queue_printed = true
        else
          line = " ".ljust(COLUMN_QUEUE_WIDTH) + COLUMN_SPACING
          line += directory.to_s
          puts line
        end
      end
      queue_number += 1
      puts
    end
  else
    puts  "### Execution will performed on a single thread."
  end

end


### Process received options [[[
def distribute_workload(execution_tree, threads)
  directories_per_queue = (execution_tree.length/threads.to_f).ceil
  queues = []
  execution_tree.keys.each_slice(directories_per_queue) do |key_slice|
    thread_queue = {}
    key_slice.each do |key|
      thread_queue[key] = execution_tree[key]
    end
    queues << thread_queue
  end

  return queues
end


def begin_execution(queues, options)
  ### Print headers [[[
  header  = COLUMN_ALIGNMENT_TITLE.ljust(@column_alignment_width) + COLUMN_SPACING
  header += COLUMN_ITERATION_TITLE.ljust(COLUMN_ITERATION_WIDTH)  + COLUMN_SPACING
  if options[:verbose]
    header += COLUMN_EVENT_TITLE.ljust(COLUMN_EVENT_WIDTH)        + COLUMN_SPACING  
  end
  header += COLUMN_TIME_TITLE.ljust(COLUMN_TIME_WIDTH)
  puts header
  # ]]]

  threads = []
  queues.each do |workload|
    threads << Thread.new{ process_workload(workload, options) }
  end

  threads.each do |thread|
    thread.join
  end
end


def process_workload(workload, options)
  workload.each do |directory, iterations|
    files = {}
    total_time = 0

    begin
      iterations.each do |current_iteration, iteration_options|
        files[current_iteration] = {}
        failure = {}

        if directory.eql? "."
          directory_name = Pathname.new(directory).expand_path.basename.to_s
        else
          directory_name = directory
        end

        msg  = directory_name.ljust(@column_alignment_width) + COLUMN_SPACING
        msg += current_iteration.to_s.ljust(COLUMN_ITERATION_WIDTH) + COLUMN_SPACING

        ### Generate control file [[[
        control_file_basename = FILE_COMMON_PREFIX + current_iteration.to_s + FILE_CONTROL_SUFFIX
        control_file_path = Pathname.new(directory + "/" + control_file_basename).expand_path
        files[current_iteration][:control] = control_file_path
        File.open(control_file_path, 'w') do |file|
          file.puts CONTROL_FILE_START

          # If it starts with CONFIG_FILE_MARKER then parsing is required. Since glob
          # expansion was done earlier, at this point we only care about extraction options
          iteration_options.each do |key, value|
            if value.is_a? String and value.start_with? CONFIG_FILE_MARKER
              matches = REGEX_EXTRACTION.match(value)
              if matches.size.eql? 0
                raise ExtractionError, "#{value} could not be parsed"
              end
              source_iteration, keyword = matches.captures

              if not keyword.is_a? Regexp
                keyword = /#{keyword}/ 
              end

              source_file = files[source_iteration.to_i][:outfile]
              value = extract_value_from(keyword, source_file)

              if value.nil?
                msg += "Error! Failed to extract #{key} from #{source_file}. Skipping remaining iterations for this alignment"
                raise ExtractionError
              end

            end

            file.puts format_option(key, value)
          end

          file.puts CONTROL_FILE_END
          file.puts

          # If a template was specified, append its contents
          if options[:template]
            File.open(options[:template]) do |template|
              template.each_line do |line|
                file.puts line
              end
            end
          end
        end
        ### ]]]

        # Parse this iteration's control file for the output file name and store it for possible
        # later extractions
        current_output_file = directory.to_s + "/" + extract_value_from(/outfile/, control_file_path)
        files[current_iteration][:outfile] = Pathname.new(files[current_iteration][:control]).expand_path.to_s

        if options[:verbose]
          msg_start  = msg + COLUMN_EVENT_START.ljust(COLUMN_EVENT_WIDTH) + COLUMN_SPACING
          threaded_puts(msg_start)
        end

        cmd = "cd #{directory}; codeml #{files[current_iteration][:control]}"
        start_time = Time::now 

        file_stdout = FILE_COMMON_PREFIX + current_iteration.to_s + FILE_STDOUT_SUFFIX
        file_stderr = FILE_COMMON_PREFIX + current_iteration.to_s + FILE_STDERR_SUFFIX
        system(cmd + ">#{file_stdout} 2>#{file_stderr}")

        path_stderr = directory + "/" + file_stderr
        path_stdout = directory + "/" + file_stdout

        if File.exists? path_stderr and File.zero? path_stderr
          File.delete path_stderr
        end
        
        runtime = Time::now - start_time
        total_time += runtime

        # Experimental
        if not $?.exitstatus.eql? 0
          failure[:source] = :codeml
          reason = "Exit status was #{$?.exitstatus}. Last line of output: \"#{File.open(path_stdout).to_a.last.delete("\n")}\""
          #reason = file_stdout + ': "' +  + '"'
          failure[:reason] = reason
        else
          File.open(path_stdout).each_line do |line|
            if REGEX_ERROR_DETECTION.match(line)
              failure[:source] = :codeml
              reason = "Error detected in output: \"#{line.delete("\n")}\""
              failure[:reason] = reason
            end
          end
        end
        

        if failure.empty?
          if options[:verbose]
            msg += COLUMN_EVENT_SUCCESS.ljust(COLUMN_EVENT_WIDTH) + COLUMN_SPACING
          end
          msg += "#{human_readable_time(runtime)} / #{human_readable_time(total_time)}"
          threaded_puts(msg)

        else
          failure[:directory] = directory_name
          failure[:iteration] = current_iteration
          @failed_directories << failure
          if options[:verbose]
            msg += COLUMN_EVENT_FAILURE.ljust(COLUMN_EVENT_WIDTH) + COLUMN_SPACING
          end
          msg += "#{human_readable_time(runtime)} / #{human_readable_time(total_time)}".ljust(COLUMN_TIME_WIDTH) + COLUMN_SPACING
          threaded_puts(msg)
          raise ExecutionError
          #raise ExecError, out.split("\n").grep(CODEML_ERRORS).join("|")
        end
      end
    rescue ExtractionError, ExecutionError => e
      next
    rescue Exception => e
      threaded_puts "Unknown error during execution of Clarisse"
      threaded_puts e.message + "\n" + e.backtrace.join("\n")
      abort
    end
  end
end

def summarize(options)
  # Summarize failures, if any
  if @failed_directories.empty?
    if options[:verbose]
      puts
      puts "Successfully executed all iterations"
    end
  else
    puts "\n" * 3
    puts "            --- ERROR REPORT ---"
    puts

    header  = COLUMN_ALIGNMENT_TITLE.ljust(@column_alignment_width) + COLUMN_SPACING
    header += COLUMN_ITERATION_TITLE.ljust(COLUMN_ITERATION_WIDTH)  + COLUMN_SPACING
    header += COLUMN_FAILURE_SOURCE_TITLE.ljust(COLUMN_FAILURE_SOURCE_WIDTH)      + COLUMN_SPACING
    header += COLUMN_FAILURE_REASON_TITLE.ljust(COLUMN_FAILURE_REASON_WIDTH)      + COLUMN_SPACING
    puts header

    @failed_directories.each do |directory|
      line  = directory[:directory].ljust(@column_alignment_width)      + COLUMN_SPACING
      line += directory[:iteration].to_s.ljust(COLUMN_ITERATION_WIDTH)  + COLUMN_SPACING
      line += COLUMN_FAILURE_SOURCE[directory[:source]].ljust(COLUMN_FAILURE_SOURCE_WIDTH)  + COLUMN_SPACING
      line += directory[:reason].ljust(COLUMN_FAILURE_SOURCE_WIDTH)            + COLUMN_SPACING
      puts line
    end

    puts
    puts "#{@failed_directories.length} errors"
  end
end

# Each thread will receive its own directory_list and a common iteration_list
def threaded_puts(message)
  @mutex.synchronize {
    puts message
  }
end

##### Extract a value from a results file to be used in a control file
def extract_value_from(keyword, source)
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


def human_readable_time(seconds)
  seconds = seconds.to_i
  hours = seconds/3600
  seconds %= 3600
  minutes = seconds/60
  seconds %= 60

  return  hours.to_s.rjust(2,'0') + ":" + minutes.to_s.rjust(2,'0') + ":" +
          seconds.to_s.rjust(2,'0')
end


# Start execution
clarisse_main()
