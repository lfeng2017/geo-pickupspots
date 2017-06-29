#!/usr/bin/env bash

# usage: export ENV=pro && sh pipeline.sh bj


function log()
{
    time=`date +%Y/%m/%d-%H:%M:%S`
    echo "[$1] $time $2"
}

if [ ! -n "$1" ] ;then
    log "ERROR" "you have not input a city"
    exit -1
else
    log "INFO" "the city you input is $1"
fi


if [ ! -n "$ENV" ] ;then
    log "WARN" "no ENV setting, set ENV=dev/pro firstly"
    exit -1
fi
log "INFO" "running ENV=$ENV"


log "INFO" "StartPositionChain processing ... "
python StartPositionChain.py --city=$1
if [ $? -eq 0 ] ;then
    log "INFO" "StartPositionChain complete"
else
    log "INFO" "StartPositionChain failed, batch job will exit with -1"
    exit -1
fi

log "INFO" "RecommPosLocally processing ... "
python RecommPosLocally.py --city=$1 --clear=True
if [ $? -eq 0 ] ;then
    log "INFO" "RecommPosLocally complllete"
else
    log "INFO" "RecommPosLocally failed, batch job will exit with -1"
    exit -1
fi

log "INFO" "NamingLocally processing ... "
python NamingLocally.py --city=$1 --clear=True
if [ $? -eq 0 ] ;then
    log "INFO" "NamingLocally complete"
else
    log "INFO" "NamingLocally failed, batch job will exit with -1"
    exit -1
fi

log "INFO" "PersistRecommPos processing ... "
python PersistRecommPos.py --city=$1
if [ $? -eq 0 ] ;then
    log "INFO" "PersistRecommPos complete"
else
    log "INFO" "PersistRecommPos failed, batch job will exit with -1"
    exit -1
fi

log "NOTICE" "all is done"

exit 0

