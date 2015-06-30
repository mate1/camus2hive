#!/usr/bin/env bash

set -e

usage() { echo "Usage: $0 -r schema-registry-url -t topic -f FILE" 1>&2; exit 1; }


while getopts ":r:t:f:" o; do
    case "${o}" in
        r)
            registry=${OPTARG}
            ;;
        t)
            subject_type="${OPTARG}-value"
            ;;
        f)
            schema_file=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))


[ -z "${registry}" ] && usage
[ -z "${subject_type}" ] && usage
[ -z "${schema_file}" ] && usage

if [ `uname -s` == "Darwin" ]
then
    temp_file=`mktemp -t XX`
else
    temp_file=`mktemp`
fi


schema_data=`cat $schema_file | jq -c  '@text | {schema: .}' > ${temp_file}`

echo ${schema_data} EOF | curl -vvv \
    -H "Content-Type: application/vnd.schemaregistry.v1+json"\
    -L \
    -X POST \
    --data-binary "@${temp_file}" \
    "${registry}/subjects/${subject_type}/versions"


function finish {
    rm -f ${temp_file}
}

trap finish EXIT
