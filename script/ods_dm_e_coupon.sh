#!/bin/bash
cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env
source ${util_dir}/my_functions

exec_sql="SELECT 
              id,
              mobile_number,
              order_type,
              general,
              actual_price,
              server_fee,
              price,
              exchange_type,
              denomination,
              exchange_price,
              shop_id,
              shop_name,
              operator_name,
              operator_id,
              client_code,
              lock_order_number,
              DATE_FORMAT(created_time,'%Y-%m-%d') as dt
          FROM e_coupon
          WHERE \$CONDITIONS AND (DATE_FORMAT(created_time,'%Y-%m-%d') BETWEEN '$d90' AND '$d1')"

if [ -n "${table_tmp_ods_e_coupon}" ] && [ ${table_tmp_ods_e_coupon} != "/" ]; then
  target_dir="${OSS_URL}${tmp_hive_dir}${table_tmp_ods_e_coupon}"
else
  exit 255
fi

import_data_hdfs "${exec_sql}" "${target_dir}" "${jdbc_url_rds1}" "${jdbc_username_rds1}" "${jdbc_passwd_rds1}"
check_success

hql="set hive.exec.dynamic.partition.mode=nonstrict;
     set mapred.job.name="ods_dm_e_coupon_mapred_tasks";
     set mapred.reduce.tasks=30;
     insert overwrite table ${table_ods_e_coupon} partition(dt)
     select * from ${table_tmp_ods_e_coupon}"

echo_ex "${hql}"
${HIVE} -e "${hql}"

check_success
exit 0
