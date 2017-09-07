#!/usr/bin/python

import os
import sys
import re
import logging
import optparse
import time
import shlex

def runNum (line):

	pat = re.compile('file_(\d{4})_')
	run = pat.search(line)
	return run.group(1)
#end def

def writePBS (run):

	#
	# Prepare file names
	baseName = 'file_' + run
	name = '/home/user_name/pbs/control/' + baseName + '.pbs'

	#
	# Open the pbs file for writing
	f = open(name, 'w')

	#
  # Write the pbs file

	# Write the directives section
	f.write('#!/bin/bash\n')
	f.write('\n')
	f.write('# job name:\n')
	f.write('#PBS -N project_run_' + run\n')
	f.write('\n')
	f.write('# maximum wall time for the job\n')
	f.write('#PBS -l walltime=15:00:00' + '\n')
	f.write('\n')
	f.write('# maximum amount of memory\n')
	f.write('#PBS -l mem=4000mb\n')
	f.write('\n')
	f.write('# path/filename for standard output\n')
	f.write('#PBS -o /home/user_name/scratch/pbs/output/' + baseName + '.out' + '\n')
	f.write('\n')
	f.write('# path/filename for standard error\n')
	f.write('#PBS -e /home/user_name/scratch/pbs/output/' + baseName + '.err' + '\n')
	f.write('\n')
	f.write('# export all my environment variables to the job\n')
	f.write('#PBS -V' + '\n')
	f.write('\n')

  # Write the body
	f.write('uname -a\n')
	f.write('echo $libraries_path\n')
	f.write('\n')

	f.write('source /home/user_name/program/setup.sh\n')
	f.write('\n')

	f.write('cd /home/user_name/scratch/data/processed\n')

	f.write('time program.exe --options ' + '-o output_file' + run + '\n')
	f.write('\n')

	f.write('echo Job Completed Successfully\n')

	f.close()
	return name
#end def

#
# Parse the input file names from the comand line:
print "   Checking command line inputs..."
optionParser = optparse.OptionParser()
(options, args) = optionParser.parse_args()
if len(args) < 1:
  print "Usage is:"
  print "python cluster_jobs.py [input file] [start line num] [end line num]"
  sys.exit(0)
#end if len(

start = 0
end = 999999999
if len(args) == 3:
	print "   Setting first and lines to process..."
	start = int(args[1])
	end = int(args[2])

print "   Generating .pbs files..."
count = 0
file = open(args[0], 'r')
for line in file:

	if count >= start and count <= end:

		line = line.rstrip('\r\n')
		run = runNum(line)
		job = writePBS(run)

		os.system('qsub ' + job)
		time.sleep(1)

	elif count > end:
		break
	#end if

	count += 1
#end for line
print "   Finished. Exiting..."
