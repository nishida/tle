#!/bin/bash

# MY ENVIRONMENT
export NEW_VIRTUAL_ENV=/nwork/nishida/space_weather/virtualenv/python38-tle
. ~/.bashrc_python38

set -e -o pipefail
trap 'echo $(date -R) Error at line ${LINENO[0]} in ${BASH_SOURCE:-$0}' ERR

cd $(dirname $0)
readonly BASE_PATH=$(pwd)
readonly DOWNLOAD_DIR=${BASE_PATH}/download
readonly SATCAT_FILE=${DOWNLOAD_DIR}/satcat_latest.json
readonly DATABASE1=${BASE_PATH}/db/elset.sqlite3
readonly TABLE1=elset
readonly DATABASE2=${BASE_PATH}/db/elset_with_tle.sqlite3 
readonly TABLE2=elset
readonly DATABASE3=${BASE_PATH}/db/satcat.sqlite3
readonly TABLE3=satcat

readonly NDAYS=2
readonly TODAY=$(TZ=UTC0 date -I)
readonly DATE1=$(TZ=UTC0 date -I -d "${TODAY} - ${NDAYS} days + 1 day")
readonly DATE2=$(TZ=UTC0 date -I -d "${TODAY}")
declare -a FILES=()
for (( i = 0; i < ${NDAYS} ; i++ )) ; do
	DATESTR=$(TZ=UTC0 date +%Y%m%d -d "${DATE1} + $i days")
	FILES+=( ${DOWNLOAD_DIR}/${DATESTR}.json.xz )
done

mkdir -p $DOWNLOAD_DIR

# Download ELSET
./download_gp_date.py -f -c ${DATE1} ${DATE2} > /dev/null 2>&1

# UPSERT to Database
./json2sqlite3.py "${FILES[@]}" ${DATABASE1} ${TABLE1} > /dev/null 2>&1
./json2sqlite3.py -t "${FILES[@]}" ${DATABASE2} ${TABLE2} > /dev/null 2>&1

# Download SATCAT
./download_satcat.py -f -o ${SATCAT_FILE} > /dev/null 2>&1

# UPSERT to Database
./satcat2sqlite3.py ${SATCAT_FILE}.xz ${DATABASE3} ${TABLE3} > /dev/null 2>&1

exit 0

