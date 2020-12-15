#!/bin/bash
# @date: 2018-12-17
cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

function pull_table_partition() {

  db_table="$1"
  id="$2"
  partition_time_field="$id"

  db_name=`echo $db_table | awk -F '.' '{print $1}'`
  t_name=`echo $db_table | awk -F '.' '{print $2}'`

  exec_sql="SELECT
              *
            FROM ${db_table}
  	    WHERE \$CONDITIONS AND (DATE_FORMAT(${partition_time_field},'%Y-%m-%d') = '$d1')"
  
  if [ -n "${t_name}" ] && [ ${t_name} != "/" ]; then
    target_dir="${ext_dc_dir}/stg_${db_name}/${t_name}/dt=${d1}"
  else
    exit 255
  fi
  import_data_hdfs "${exec_sql}" "${target_dir}" "${jdbc_url_87}" "${jdbc_username_87}" "${jdbc_passwd_87}" "${id}"

  check_success

  hive -e "alter table stg_${db_name}.${t_name} add partition(dt='$d1');"
}

# '''partition'''

pull_table_partition loanuser.user_customer_resource_relation CREATE_TIME
pull_table_partition loanuser.user_financial_instruments_info CREATE_TIME
pull_table_partition loanuser.user_customer_relation CREATE_TIME
pull_table_partition loanuser.user_personal_basic_info CREATE_TIME
pull_table_partition loanuser.user_tel_contact_station_info CREATE_TIME
pull_table_partition loanuser.user_customer_role_relation CREATE_TIME
pull_table_partition loanuser.user_recommend_serial CREATE_TIME
pull_table_partition loanuser.user_address_contact_station_info CREATE_TIME
pull_table_partition loanuser.user_contact_station_info CREATE_TIME
pull_table_partition loanuser.user_resource_info CREATE_TIME
pull_table_partition asset.asset_loan_invoice_info CREATE_TIME
pull_table_partition asset.asset_repay_plan CREATE_TIME
pull_table_partition asset.asset_loan_serial CREATE_TIME
pull_table_partition asset.asset_fee_rule CREATE_TIME
pull_table_partition asset.asset_pay_detail CREATE_TIME
pull_table_partition asset.asset_repay_serial CREATE_TIME
pull_table_partition credittrans.credit_loan_apply CREATE_TIME
pull_table_partition credittrans.credit_credit_apply CREATE_TIME
pull_table_partition credittrans.credit_cust_mgr_credit_loan_apply CREATE_TIME
pull_table_partition credittrans.credit_into_apply CREATE_TIME
pull_table_partition credittrans.credit_credit_info CREATE_TIME
pull_table_partition credittrans.credit_loan_merchant_info CREATE_TIME
pull_table_partition product.product_product CREATE_TIME

exit 0
