#!/usr/bin/env ruby
$LOAD_PATH << File.join(File.expand_path(File.dirname(File.symlink?(__FILE__) ? File.readlink(__FILE__) : __FILE__)), 'lib')

require 'fileutils'
require 'open-uri'
require 'thread'
require 'trollop'
require 'yaml'

USAGE = <<END
Usage : scald.rb [--cp classpath] [--jar jarfile] [--hdfs|--hdfs-local|--local|--print] job <job args>
 --cp: scala classpath
 --clean: clean rsync and maven state before running
 --jar ads-batch: specify the jar file
 --hdfs: if job ends in ".scala" or ".java" and the file exists, link it against JARFILE (below) and then run it on HOST.
         else, it is assumed to be a full classname to an item in the JARFILE, which is run on HOST
 --hdfs-local: run in hadoop local mode (--local is cascading local mode)
 --host: specify the hadoop host where the job runs
 --local: run in cascading local mode (does not use hadoop)
 --print: print the command YOU SHOULD ENTER on the remote node. Useful for screen sessions.
END

##############################################################
# Default configuration:
  #Get the absolute path of the original (non-symlink) file.
CONFIG_DEFAULT = begin
  original_file = File.symlink?(__FILE__) ? File.readlink(__FILE__) : __FILE__
  repo_root = File.expand_path(File.dirname(original_file)+"/../")
  { "host" => "my.host.here", #where the job is rsynced to and run
    "repo_root" => repo_root, #full path to the repo you use, Twitter specific
    "cp" => ENV['CLASSPATH'] || "",
    "localmem" => "3g", #how much memory for java to use when running in local mode
    "namespaces" => { "abj" => "com.twitter.ads.batch.job", "s" => "com.twitter.scalding" },
    "hadoop_opts" => { "mapred.reduce.tasks" => 20, #be conservative by default
                       "mapred.min.split.size" => "2000000000" }, #2 billion bytes!!!
    "depends" => [ "org.apache.hadoop/hadoop-core/0.20.2",
                   "log4j/log4j/1.2.15",
                   "commons-httpclient/commons-httpclient/3.1",
                   "commons-cli/commons-cli/1.2",
                   "org.apache.zookeeper/zookeeper/3.3.4" ],
    "default_mode" => "--hdfs"
  }
end
##############################################################

CONFIG_RC = begin
#put configuration in .scaldrc in HOME to override the defaults below:
    YAML.load_file(ENV['HOME'] + "/.scaldrc") || {} #seems that on ruby 1.9, this returns false on failure
  rescue
    {}
  end

CONFIG = CONFIG_DEFAULT.merge!(CONFIG_RC)

BUILDFILE = open(CONFIG["repo_root"] + "/build.sbt").read
SCALDING_VERSION=BUILDFILE.match(/version\s*:=\s*\"([^\"]+)\"/)[1]
SCALA_VERSION=BUILDFILE.match(/scalaVersion\s*:=\s*\"([^\"]+)\"/)[1]

if (!CONFIG["jar"])
  #what jar has all the depencies for this job
  CONFIG["jar"] = repo_root + "/target/scalding-assembly-#{SCALDING_VERSION}.jar"
end

#Check that we can find the jar:
if (!File.exist?(CONFIG["jar"]))
  puts("#{CONFIG["jar"]} is missing, you probably need to run sbt assembly")
  exit(1)
end

#optionally set variables (not linux often doesn't have this set, and falls back to TMP. Set up a
#YAML file in .scaldrc with "tmpdir: my_tmp_directory_name" or export TMPDIR="/my/tmp" to set on
#linux
TMPDIR=CONFIG["tmpdir"] || ENV['TMPDIR'] || "/tmp"
TMPMAVENDIR = File.join(TMPDIR, "maven")
BUILDDIR=CONFIG["builddir"] || File.join(TMPDIR,"script-build")
LOCALMEM=CONFIG["localmem"] || "3g"
DEPENDENCIES=CONFIG["depends"] || []
RSYNC_STATFILE_PREFIX = TMPDIR + "/scald.touch."

#Recall that usage is of the form scald.rb [--jar jarfile] [--hdfs|--hdfs-local|--local|--print] job <job args>
#This parser holds the {job <job args>} part of the command.
OPTS_PARSER = Trollop::Parser.new do
  opt :clean, "Clean all rsync and maven state before running"
  opt :cp, "Scala classpath", :type => String
  opt :hdfs, "Run on HDFS"
  opt :hdfs_local, "Run in Hadoop local mode"
  opt :local, "Run in Cascading local mode (does not use Hadoop)"
  opt :print, "Print the command YOU SHOULD enter on the remote node. Useful for screen sessions"

  opt :jar, "Specify the jar file", :type => String
  opt :host, "Specify the hadoop host where the job runs", :type => String
  opt :reducers, "Specify the number of reducers", :type => :int

  stop_on_unknown #Stop parsing for options parameters once we reach the job file.
end

#OPTS holds the option parameters that come before {job}, i.e., the
#[--jar jarfile] [--hdfs|--hdfs-local|--local|--print] part of the command.
OPTS = OPTS_PARSER.parse ARGV

#Make sure one of the execution modes is set.
unless [OPTS[:hdfs], OPTS[:hdfs_local], OPTS[:local], OPTS[:print]].any?
  #Modes in CONFIG file are in the form "--hdfs" or "--local", but the OPTS hash assumes
  #them to be in the form :hdfs or :local.
  #TODO: Add error checking on CONFIG["default_mode"]?
  mode = CONFIG["default_mode"] ? CONFIG["default_mode"].gsub("--", "").to_sym : :hdfs
  OPTS[mode] = true
end

def maven_filename(jar_filename)
  File.join(TMPMAVENDIR, jar_filename)
end

if OPTS[:clean]
  rsync_files = Dir.glob(RSYNC_STATFILE_PREFIX + "*")
  $stderr.puts("Cleaning rsync stat files: #{rsync_files.join(', ')}")
  rsync_files.each { |f| File.delete(f) }

  maven_files = Dir.glob(maven_filename("*"))
  $stderr.puts("Cleaning maven jars: #{maven_files.join(', ')}")
  maven_files.each { |f| File.delete(f) }
  Dir.rmdir(TMPMAVENDIR) if File.directory?(TMPMAVENDIR)
  #HACK -- exit immediately because other parts of this script assume more
  #arguments are passed in.
  exit(0)
end

if ARGV.size < 1
  $stderr.puts USAGE
  Trollop::options
  Trollop::die "insufficient arguments passed to scald.rb"
end

SBT_HOME="#{ENV['HOME']}/.sbt"
SCALA_LIB="#{SBT_HOME}/boot/scala-#{SCALA_VERSION}/lib/scala-library.jar"
COMPILE_CMD="java -cp #{SCALA_LIB}:#{SBT_HOME}/boot/scala-#{SCALA_VERSION}/lib/scala-compiler.jar -Dscala.home=#{SBT_HOME}/boot/scala-#{SCALA_VERSION}/lib/ scala.tools.nsc.Main"

HOST = OPTS[:host] || CONFIG["host"]

CLASSPATH =
  if OPTS[:cp]
    CONFIG["cp"] + ":" + OPTS[:cp]
  else
    CONFIG["cp"]
  end

JARFILE =
  if OPTS[:jar]
    jarname = OPTS[:jar]
    #highly Twitter specific here:
    CONFIG["repo_root"] + "/dist/#{jarname}-deploy.jar"
  else
    CONFIG["jar"]
  end

JOBFILE=OPTS_PARSER.leftovers.first
JOB_ARGS=OPTS_PARSER.leftovers[1..-1].join(" ")

#Check that we have all the dependencies, and download any we don't.
def maven_get(dependencies = DEPENDENCIES)
  #First, make sure the TMPMAVENDIR exists and create it if not.
  FileUtils.mkdir_p(TMPMAVENDIR)

  #Now make sure all the dependencies exist.
  dependencies.each do |dependency|
    jar_filename = dependency_to_jar(dependency)

    #Check if we already have the jar. Get it if not.
    if !File.exists?(maven_filename(jar_filename))
      url = dependency_to_url(dependency)
      uri = URI(dependency_to_url(dependency))
      $stderr.puts("downloading #{jar_filename} from #{uri}...")

      File.open(maven_filename(jar_filename), "wb") do |f|
        begin
          f.print open(url).read
          $stderr.puts "Successfully downloaded #{jar_filename}!"
        rescue SocketError => e
          $stderr.puts "SocketError in downloading #{jar_filename}: #{e}"
        rescue SystemCallError => e
          $stderr.puts "SystemCallError in downloading #{jar_filename}: #{e}"
        end
      end
    end
  end
end

#Converts an array of dependencies into an array of jar filenames.
#Example:
#Input dependencies: ["org.apache.hadoop/hadoop-core/0.20.0", "com.twitter/scalding/0.2.0"]
#Output jar filenames: ["/tmp/mvn/hadoop-core-0.20.0.jar","/tmp/mvn/scalding-0.2.0.jar"]
def convert_dependencies_to_jars(dependencies = DEPENDENCIES, as_classpath = false)
  ret = dependencies.map{ |d| maven_filename(dependency_to_jar(d)) }
  ret = ret.join(":") if as_classpath
  ret
end

#Convert a maven dependency to a url for downloading.
#Example:
#Input dependency: org.apache.hadoop/hadoop-core/0.20.2
#Output url: http://repo1.maven.org/maven2/org/apache/hadoop/hadoop-core/0.20.2/hadoop-core-0.20.2.jar
def dependency_to_url(dependency)
  #Each dependency is in the form group/artifact/version.
  group, artifact, version = dependency.split("/")
  jar_filename = dependency_to_jar(dependency)
  group_with_slash = group.split(".").join("/")
  "http://repo1.maven.org/maven2/#{group_with_slash}/#{artifact}/#{version}/#{jar_filename}"
end

#Convert a maven dependency to the name of its jar file.
#Example:
#Input dependency: org.apache.hadoop/hadoop-core/0.20.2
#Output: hadoop-core-0.20.2.jar
def dependency_to_jar(dependency)
  group, artifact, version = dependency.split("/")
  "#{artifact}-#{version}.jar"
end

def hadoop_opts
  opts = CONFIG["hadoop_opts"] || {}
  opts["mapred.reduce.tasks"] = OPTS[:reducers] if OPTS[:reducers]
  if !opts.has_key?("mapred.reduce.tasks")
    Trollop::die "number of reducers not set"
  end
  opts.collect { |k,v| "-D#{k}=#{v}" }.join(" ")
end

def file_type
  JOBFILE =~ /\.(scala|java)$/
  $1
end

def is_file?
  !file_type.nil?
end

EXTENSION_RE = /(.*)\.(scala|java)$/

#Get the name of the job from the file.
#the rule is: last class in the file, or the one that matches the filename
def get_job_name(file)
  package = ""
  job = nil
  default = nil
  if file =~ EXTENSION_RE
    default = $1
    File.readlines(file).each { |s|
      if s =~ /^package ([^;]+)/
        package = $1.chop + "."
      elsif s =~ /class\s+([^\s(]+).*extends\s+.*Job/
        unless job and default and (job.downcase == default.downcase)
          #use either the last class, or the one with the same name as the file
          job = $1
        end
      end
    }
    raise "Could not find job name" unless job
    "#{package}#{job}"
  elsif file =~ /(.*):(.*)/
    begin
      CONFIG["namespaces"][$1] + "." + $2
    rescue
      $stderr.puts "Unknown namespace: #{$1}"
      exit(1)
    end
  else
    file
  end
end

JARPATH=File.expand_path(JARFILE)
JARBASE=File.basename(JARFILE)
JOBPATH=File.expand_path(JOBFILE)
JOB=get_job_name(JOBFILE)
JOBJAR=JOB+".jar"
JOBJARPATH=TMPDIR+"/"+JOBJAR


class ThreadList
  def initialize
    @threads = []
    @failures = []
    @mtx = Mutex.new
    @open = true
  end

  def thread(*targs, &block)
    @mtx.synchronize {
      if @open
        @threads << Thread.new(*targs, &block)
      else
        raise "ThreadList is closed"
      end
    }
  end

  def failure(f)
    @mtx.synchronize {
      @failures << f
    }
  end

  #if the size > 0, execute a block then join, else no not yield.
  #returns thread count
  def waitall
    block = false
    size = @mtx.synchronize {
      if @open
        @open = false
        block = true
      end
      @threads.size
    }
    if block
      yield size
      @threads.each { |t| t.join }
      #at this point, all threads are finished.
      if @failures.each { |f| $stderr.puts f }.size > 0
        raise "There were failures"
      end
    end
    size
  end
end

THREADS = ThreadList.new

# Returns file size formatted with a human-readable suffix.
def human_filesize(filename)
  size = File.size(filename)
  case
  when size < 2**10 then '%d bytes' % size
  when size < 2**20 then '%.1fK'    % (1.0 * size / 2**10)
  when size < 2**30 then '%.1fM'    % (1.0 * size / 2**20)
  else                   '%.1fG'    % (1.0 * size / 2**30)
  end
end

#this is used to record the last time we rsynced
def rsync_stat_file(filename)
  RSYNC_STATFILE_PREFIX+filename.gsub(/\//,'.')+"."+HOST
end

#In another thread, rsync the file. If it succeeds, touch the rsync_stat_file
def rsync(from, to)
  rtouch = rsync_stat_file(from)
  if !File.exists?(rtouch) || File.stat(rtouch).mtime < File.stat(from).mtime
    $stderr.puts("rsyncing #{human_filesize(from)} from #{to} to #{HOST} in background...")
    THREADS.thread(from, to) { |ff,tt|
      if system("rsync -e ssh -z #{ff} #{HOST}:#{tt}")
        #this indicates success and notes the time
        FileUtils.touch(rtouch)
      else
        #indicate failure
        THREADS.failure("Could not rsync: #{ff} to #{HOST}:#{tt}")
        FileUtils.rm_f(rtouch)
      end
    }
  end
end

def is_local?
  OPTS[:local] || OPTS[:hdfs_local]
end
def needs_rebuild?
  if !File.exists?(JOBJARPATH)
    true
  else
    #the jar exists, but is it fresh enough:
    mtime = File.stat(JOBJARPATH).mtime
    [ # if the jar is older than the path of the job file:
      (mtime < File.stat(JOBPATH).mtime),
      # if the jobjar is older than the main jar:
      mtime < File.stat(JARPATH).mtime ].any?
  end
end

def build_job_jar
  $stderr.puts("compiling " + JOBFILE)
  FileUtils.mkdir_p(BUILDDIR)
  classpath = (convert_dependencies_to_jars +
               ([SCALA_LIB, JARPATH, CLASSPATH].select { |s| s != "" })).join(":")
  puts("#{file_type}c -classpath #{classpath} -d #{BUILDDIR} #{JOBFILE}")
  unless system("#{COMPILE_CMD} -classpath #{classpath} -d #{BUILDDIR} #{JOBFILE}")
    FileUtils.rm_f(rsync_stat_file(JOBJARPATH))
    FileUtils.rm_rf(BUILDDIR)
    exit(1)
  end

  FileUtils.rm_f(JOBJARPATH)
  system("jar cf #{JOBJARPATH} -C #{BUILDDIR} .")
  FileUtils.rm_rf(BUILDDIR)
end

def hadoop_command
  "HADOOP_CLASSPATH=/usr/share/java/hadoop-lzo-0.4.15.jar:#{JARBASE}:job-jars/#{JOBJAR} " +
    "hadoop jar #{JARBASE} -libjars job-jars/#{JOBJAR} #{hadoop_opts} #{JOB} --hdfs " +
    JOB_ARGS
end

def jar_mode_command
  "hadoop jar #{JARBASE} #{hadoop_opts} #{JOB} --hdfs " + JOB_ARGS
end

#Always sync the remote JARFILE
rsync(JARPATH, JARBASE) if !is_local?
#make sure we have the dependencies to compile and run locally (these are not in the above jar)
#this does nothing if we already have the deps.
maven_get
if is_file?
  build_job_jar if needs_rebuild?

  if !is_local?
    #Make sure the job-jars/ directory exists before rsyncing to it
    system("ssh #{HOST} '[ ! -d job-jars/ ] && mkdir job-jars/'")
    #rsync only acts if the file is out of date
    rsync(JOBJARPATH, "job-jars/" + JOBJAR)
  end
end

def local_cmd(mode)
  classpath = (convert_dependencies_to_jars + [JARPATH]).join(":") + (is_file? ? ":#{JOBJARPATH}" : "") +
                ":" + CLASSPATH
  "java -Xmx#{LOCALMEM} -cp #{classpath} com.twitter.scalding.Tool #{JOB} #{mode} " + JOB_ARGS
end

SHELL_COMMAND =
  if OPTS[:hdfs]
    if is_file?
      "ssh -t -C #{HOST} #{hadoop_command}"
    else
      "ssh -t -C #{HOST} #{jar_mode_command}"
    end
  elsif OPTS[:hdfs_local]
    local_cmd("--hdfs")
  elsif OPTS[:local]
    local_cmd("--local")
  elsif OPTS[:print]
    if is_file?
      "echo #{hadoop_command}"
    else
      "echo #{jar_mode_command}"
    end
  else
    Trollop::die "no mode set"
  end

#Now block on all the threads:
begin
  THREADS.waitall { |c| puts "Waiting for #{c} background thread#{c > 1 ? 's' : ''}..." if c > 0 }
  #If there are no errors:
  exit(system(SHELL_COMMAND))
rescue
  exit(1)
end
