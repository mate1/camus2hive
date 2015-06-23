#!/usr/bash


# Dependencies validation
command -v hive >/dev/null 2>&1 || {
	echo "The hive command must be defined. Aborting."
	exit 1
}

command -v hdfs >/dev/null 2>&1 || {
	echo "The hdfs command must be defined. Aborting."
	exit 1
}

command -v kafka-topics >/dev/null 2>&1 || {
    echo "The kafka-topics command must be defined. Aborting."
    exit 1
}

function print_usage() {
    echo "Usage: `basename $0` <camus_destination_dir> [-t topic] [-d database] [-r repository_uri]"
    echo ""
    echo "camus_destination_dir"
    echo "      HDFS path where Camus stores its destination directory."
    echo ""
    echo "mode"
    echo "      Mode selection :"
    echo "          force, delete data in HDFS."
    echo "          persist, keep data in HDFS."
    echo ""
    echo "-t,--topic <topic>"
    echo "      name of topic to nuke, default is none"
    echo ""
    echo "-h,--help"
    echo "      prints this message"
    echo ""
    echo "-d,--database <database>"
    echo "      name of database to use, default is 'default'"
    echo ""
    exit 1
}

function check_success() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        echo "HDFS ERROR : Cannot delete data for $TOPIC, see $LOG_DIR for more infos." >> $NUKE_STDERR
        exit 1
    else
        return 0
    fi
}

# Process the arguments

# Remove trailing slashes (if the supplied path is just / then $CAMUS_DESTINATION_DIR will be empty, but that's ok since commands below always add a slash after anyway...)
CAMUS_DESTINATION_DIR=`echo $1 | sed -e 's%\(/\)*$%%g'`

if [[ -z "$CAMUS_DESTINATION_DIR" ]]; then
    print_usage
    exit 1
fi

MODE=$2

if [[ -z "$MODE" ]]; then #|| "$MODE" != "force" || "$MODE" != "persist" ]]; then
    echo "MOSSSSSSSSSS"
    print_usage
    exit 1
fi

shift 2

while [[ $# -gt 0 ]]; do
    opt="$1"
    shift
    current_arg="$1"
    case "$opt" in
    "-t"|"--topic")
        TOPIC=$current_arg
        shift
        ;;
    "-d"|"--database")
        DATABASE=$current_arg
        shift
        ;;
    "-h"|"--help")
        print_usage
        exit 0
        ;;
    *)
        echo "Invalid argument $opt"
        print_usage
        exit 1
        ;;
    esac
done

if [[ -z "$DATABASE" ]]; then
    DATABASE="default"
fi

# Configuration variables
HIVE="hive --database $DATABASE -S"     # Hive bin
KAFKA="kafka-topics"                    # Kafka-topic bin
ZOOKEEPER="xray01.dc1.fyber.com:2181"   # ZK Url
RETENTION=1000                          # Retention in Ms

# Dir and log files
LOG_DIR="/var/log/nuke"
NUKE_STDOUT="$LOG_DIR/nuke.log"
NUKE_STDERR="$LOG_DIR/nuke-stderr.log"

#Pre-requisite

mkdir -p $LOG_DIR

# Here we start the real s**t

${HIVE} -e "DROP table ${TOPIC}" 2>> $NUKE_STDERR

if check_success; then
    echo `date +%x\ +%X\ ` "HIVE table deleted for topic : ${TOPIC}" >> $NUKE_STDOUT

    ${KAFKA} --alter --zookeeper $ZOOKEEPER --topic $TOPIC --config retention.ms=$RETENTION 2>> $NUKE_STDERR
    if check_success; then
        echo `date +%x\ +%X\ ` "Kafka retention changed to : $RETENTION ms for topic : ${TOPIC}" >> $NUKE_STDOUT
    fi
fi

if [[ "$MODE" = "force" ]]; then
    sudo -u hdfs hdfs dfs -rm -r -f $CAMUS_DESTINATION_DIR/$TOPIC 2>> $NUKE_STDERR
    if check_success; then
        echo `date +%x\ +%X\ ` "HDFS files deleted for topic : ${TOPIC}" >> $NUKE_STDOUT
    else
        echo `date +%x\ +%X\ ` "Some errors occured while deleting data from HDFS for topic : ${TOPIC}"
    fi
fi

echo `date +%x\ +%X\ ` "Finished processing topic ${TOPIC} " >> $NUKE_STDOUT
