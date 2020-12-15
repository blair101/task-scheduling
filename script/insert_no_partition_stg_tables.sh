#!/bin/bash
# @date: 2018-12-17
cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

function pull_table_no_partition() {

  db_table="$1"
  id="$2"

  db_name=`echo $db_table | awk -F '.' '{print $1}'`
  t_name=`echo $db_table | awk -F '.' '{print $2}'`

  exec_sql="SELECT
              *
            FROM ${db_table}
  	    WHERE \$CONDITIONS"
  
  if [ -n "${t_name}" ] && [ ${t_name} != "/" ]; then
    target_dir="${ext_dc_dir}/stg_${db_name}/${t_name}"
    echo_ex $target_dir
  else
    exit 255
  fi
  import_data_hdfs "${exec_sql}" "${target_dir}" "${jdbc_url_87}" "${jdbc_username_87}" "${jdbc_passwd_87}" "${id}"
  
  check_success
}

'''no partition''' 
pull_table_no_partition reportpublic.ods_os_shop_loan_purpose SHOP_ID
pull_table_no_partition reportpublic.dim_loan_purpose LOAN_PURPOSE
pull_table_no_partition reportpublic.tmp_smy_prepay_invoice loan_invoice_id
pull_table_no_partition reportpublic.ods_asset_service_fee_final product_id

pull_table_no_partition channel.baidu_loan_rate CUR_DATE
pull_table_no_partition loanflow.flow_biz_apply id
pull_table_no_partition product.product_product_param PRODUCT_ID

pull_table_no_partition credittrans.credit_loan_invoice_apply LOAN_APPLY_ID

exit 0
