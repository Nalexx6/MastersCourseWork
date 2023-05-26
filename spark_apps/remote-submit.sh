#!/bin/bash

set -ex

function usage() {
    echo "Usage: $0 [-m <master>] [-f <file1> [-f <file2>]...] app_path [args...]" 1>&2
}

function join_array() {
    local IFS=','
    echo "$*"
}


self_dir="$(dirname "${BASH_SOURCE[0]}")"
source $self_dir/common.sh

#
# parse arguments
#

conf_path="$self_dir/../config.yaml"
files=()

while getopts ":m:f:c:h" o; do
    case "${o}" in
        m)
            master=${OPTARG}
            ;;
        f)
            files+=("$OPTARG")
            ;;
        c)
            conf_path="${OPTARG}"
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

app_path=$1
if [ -z $app_path ]; then
    usage
    exit 1
fi
shift 1


if [ -z $master ]; then
    master=$(get_master_host)
fi

if [ -z $master ]; then
    usage
    exit 1
fi

master=hadoop@${master}
init_aws_credentials


zip -FS -q -r /tmp/$workdir.zip . -x '**/__pycache__/*'
scp /tmp/${workdir}.zip $master:/tmp/
scp "$conf_path" $master:/tmp/
ssh $master "rm -rf ~/$workdir"
ssh $master "unzip -o /tmp/${workdir}.zip -d  ~/$workdir"

files=("/tmp/$(basename "$conf_path")")

ssh -n $master "
    cd $workdir;
    export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID;
    export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY;
    $self_dir/submit.sh \
        --files $(join_array ${files[@]}) \
        "$app_path" --config "/tmp/$(basename "$conf_path")" "$@" \
        1>$app_path.stdout 2>$app_path.stderr &
    disown
"
ssh -n $master "until [ -f $workdir/submit.pid ]; do sleep 1; done"
pid=$(ssh -n $master "cat $workdir/submit.pid")
ssh -n $master "tail -f --pid $pid $workdir/$app_path.{stderr,stdout}"
