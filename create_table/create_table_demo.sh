#!/bin/sh
cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

###### create ods_loan_invoice ##########

hql="create external table if not exists test2 (
    id varchar(64) comment '租户号'
)
partitioned by (dt string)
row format delimited fields terminated by '\001' collection items terminated by ',' map keys terminated by ':' lines terminated by '\n'
stored as textfile
location '/ext_dc/default/test2';"

echo "$hql"
${HIVE} -e "$hql"
