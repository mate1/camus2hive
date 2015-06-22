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

function print_usage() {
    echo "Usage: `basename $0` <camus_destination_dir> [-d database] [-r repository_uri]"
    echo ""
    echo "camus_destination_dir"
    echo "      HDFS path where Camus stores its destination directory."
    echo ""
    echo "-h,--help"
    echo "      prints this message"
    echo ""
    echo "-d,--database <database>"
    echo "      name of database to use, default is 'default'"
    echo ""
    exit 1
}

function check_sucess() {
    local status=$?
    if [[ $status -ne 0 ]]; then
        echo "HDFS ERROR : Cannot delete data for $TOPIC in HDFS, see $LOG_DIR for more infos."i
    fi
}


# Process the arguments

# Remove trailing slashes (if the supplied path is just / then $CAMUS_DESTINATION_DIR will be empty, but that's ok since commands below always add a slash after anyway...)
CAMUS_DESTINATION_DIR=`echo $1 | sed -e 's%\(/\)*$%%g'`

if [[ -z "$CAMUS_DESTINATION_DIR" ]]; then
    print_usage
    exit 1
fi

if [[ -z "$MODE" ]]; then
    print_usage
    exit 1
fi

shift 2

while [[ $# -gt 0 ]]; do
    opt="$1"
    shift
    current_arg="$1"
    case "$opt" in
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
HIVE="hive --database $DATABASE -S"


sudo -u hdfs hdfs dfs -rm -r -f $CAMUS_DESTINATION_DIR/$TOPIC 


