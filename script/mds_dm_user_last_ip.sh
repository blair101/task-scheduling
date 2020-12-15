#!/bin/bash
###############################################################################
#                                                                             
# @desc:   mds_dm_user_last_ip
#                                                                            
############################################################################### 

cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

hql="INSERT OVERWRITE TABLE ${table_user_last_ip}
    SELECT
        phone,
        collect_set(last_visit_ip)[0] as last_visit_ip,
        collect_set(last_visit_ip_province)[0] as last_visit_ip_province,
        collect_set(last_visit_ip_city)[0] as last_visit_ip_city,
        collect_set(last_visit_ip_district)[0] as last_visit_ip_district
    FROM
        ${table_tmp_user_last_ip}
    GROUP BY phone"

echo_ex "${hql}"
${HIVE} -e "${hql}"

check_success
exit 0
