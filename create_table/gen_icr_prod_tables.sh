#!/bin/sh
cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

genpy_path=${wk_dir}/create_table

function create_stg_table() {
    
    db_table="$1"
    db_name=`echo $db_table | awk -F '.' '{print $1}'`
    
    hive_database_name="stg_${db_name}"
    
    is_partition="$2"
    
    hql=`python ${genpy_path}/gen_create_table_sql.py ${db_table} ${hive_database_name} ${is_partition}`
    echo_ex "${hql}"
    hive -e "${hql}"
    check_success
}

#'''ICR_PROD'''

create_stg_table ICR_PROD.ICRMESSAGEHEADER yes
create_stg_table ICR_PROD.ICRLOANCARDINFO yes
#create_stg_table ICR_PROD.ICRCREDITCUE yes
#create_stg_table ICR_PROD.ICRLATEST2YEAROVERDUECARD yes
#create_stg_table ICR_PROD.ICRLATEST5YEAROVERDUEDETAIL yes
#create_stg_table ICR_PROD.ICRLATEST2YEAROVERDUE yes
#create_stg_table ICR_PROD.ICRLOANINFO yes
#create_stg_table ICR_PROD.ICRUNDESTORYLOANCARD yes
#create_stg_table ICR_PROD.ICRUNDESTORYSTANDARDLOANCARD yes
#create_stg_table ICR_PROD.ICRUNPAIDLOAN yes
#create_stg_table ICR_PROD.ICROVERDUESUMMARY yes
#create_stg_table ICR_PROD.ICRFELLBACKSUMMARY yes
#create_stg_table ICR_PROD.ICRGUARANTEE yes
#create_stg_table ICR_PROD.ICRGUARANTEESUMMARY yes
