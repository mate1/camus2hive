#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##################
### Camus2Hive ###
##################

# Dependencies validation
command -v hive >/dev/null 2>&1 || {
	echo "The hive command must be defined. Aborting."
	exit 1
}

command -v hdfs >/dev/null 2>&1 || {
	echo "The hdfs command must be defined. Aborting."
	exit 1
}

# Param validation
if [[ $# != 1 ]]; then
	echo "Usage: $0 camus_destination_dir"
	echo ""
	echo "camus_destination_dir: HDFS path where Camus stores its destination directory."
	echo ""
	exit 1
fi

# Remove trailing slashes (if the supplied path is just / then $CAMUS_DESTINATION_DIR will be empty, but that's ok since commands below always add a slash after anyway...)
CAMUS_DESTINATION_DIR=`echo $1 | sed -e 's%\(/\)*$%%g'`

# Behavior config
REQUERY_HADOOP_DIRS=true
EXIT_ON_ERROR=false
PRINT_HIVE_STDERR=false

# This directory and file hold state for the whole job
WORK_DIR='temp_camus2hive'
TOPIC_NAMES="$WORK_DIR/topic_names"

# These files hold state per table/topic (and are zero-ed out between each)
EXISTING_HIVE_PARTITIONS_WITH_SLASHES="$WORK_DIR/hive_partitions_with_slashes"
EXISTING_HIVE_PARTITIONS="$WORK_DIR/hive_partitions"
EXISTING_CAMUS_PARTITIONS="$WORK_DIR/camus_partitions"
HIVE_PARTITIONS_TO_ADD="$WORK_DIR/hive_partitions_to_add"
HIVE_ADD_PARTITION_STATEMENTS="$WORK_DIR/hive_add_partitions_statements"
HIVE_STDERR="$WORK_DIR/hive_stderr"

# Return 0 if everything is ok
function hive_success_check {
	MESSAGE=$1
	if [[ -s $HIVE_STDERR ]]; then
		if [[ -z $MESSAGE ]]; then
			echo "HIVE ERROR :'((( ..."
		else
			echo "HIVE ERROR: $MESSAGE"
		fi

		if $PRINT_HIVE_STDERR ; then cat $HIVE_STDERR; fi

		if $EXIT_ON_ERROR ; then exit 1; fi

		return 1
	else
		return 0
	fi
}

# Let's get to work

mkdir -p $WORK_DIR

if $REQUERY_HADOOP_DIRS ; then
	hdfs dfs -ls $CAMUS_DESTINATION_DIR/ | grep -v 'Found .* items' | sed s%.*$CAMUS_DESTINATION_DIR/%% > $TOPIC_NAMES
fi

while read topic; do
	# Zero-out the per-topic state files (probably not necessary but whatever...)
	> $EXISTING_HIVE_PARTITIONS_WITH_SLASHES
	> $EXISTING_HIVE_PARTITIONS
	> $EXISTING_CAMUS_PARTITIONS
	> $HIVE_PARTITIONS_TO_ADD
	> $HIVE_ADD_PARTITION_STATEMENTS
	> $HIVE_STDERR

	# echo "About to process topic '$topic'"

	# Check if the table already exists in Hive

	hive -S -e "SHOW PARTITIONS $topic" 1> $EXISTING_HIVE_PARTITIONS_WITH_SLASHES 2> $HIVE_STDERR

	if hive_success_check "Table '$topic' does not currently exist in Hive (or Hive returned some other error on SHOW PARTITIONS $topic)." ; then

		cat $EXISTING_HIVE_PARTITIONS_WITH_SLASHES | sed 's%/%, %g' > $EXISTING_HIVE_PARTITIONS

		# Extract all partitions currently ingested by Camus
		hdfs dfs -ls -R $CAMUS_DESTINATION_DIR/$topic | sed "s%.*$CAMUS_DESTINATION_DIR/$topic/hourly/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/.*%year=\1, month=\2, day=\3, hour=\4%" | grep "year.*" | sort | uniq > $EXISTING_CAMUS_PARTITIONS

		grep -v -f $EXISTING_HIVE_PARTITIONS $EXISTING_CAMUS_PARTITIONS > $HIVE_PARTITIONS_TO_ADD
		
		echo "$topic currently has $(cat $EXISTING_CAMUS_PARTITIONS | wc -l) partitions in Camus directories, $(cat $EXISTING_HIVE_PARTITIONS | wc -l) in Hive and thus $(cat $HIVE_PARTITIONS_TO_ADD | wc -l) left to add to Hive"

		sed "s%\(year=\([0-9]*\), month=\([0-9]*\), day=\([0-9]*\), hour=\([0-9]*\)\)%ALTER TABLE $topic ADD IF NOT EXISTS PARTITION (\1) LOCATION '$CAMUS_DESTINATION_DIR/$topic/hourly/\2/\3/\4/\5';%" < $HIVE_PARTITIONS_TO_ADD > $HIVE_ADD_PARTITION_STATEMENTS

		hive -S -f $HIVE_ADD_PARTITION_STATEMENTS > /dev/null 2> $HIVE_STDERR
		
		if hive_success_check "Some errors occurred while adding partitions to table '$topic'" && [[ -s $HIVE_PARTITIONS_TO_ADD ]]; then
			echo "$(cat $HIVE_PARTITIONS_TO_ADD | wc -l) partitions successfully added to Hive table '$topic' :D !"
		fi
	fi
	echo ""
done < $TOPIC_NAMES

echo "Finished processing $(cat $TOPIC_NAMES | wc -l) topic(s) :)"
