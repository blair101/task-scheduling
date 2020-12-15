#!/bin/bash
###############################################################################
#                                                                             
# @desc:   mds_dm_user_bhv
#                                                                            
############################################################################### 

cd `dirname $0`/.. && wk_dir=`pwd` && cd -
source ${wk_dir}/util/env

hql="INSERT OVERWRITE TABLE ${table_user_bhv}
    SELECT
        collect_set(user_id)[0] as user_id,
        phone,
        collect_set(model)[0] as model,
        collect_set(first_visit_time)[0] as first_visit_time,
        collect_set(first_visit_ip)[0] as first_visit_ip,
        collect_set(first_visit_ip_city)[0] as first_visit_ip_city,
        collect_set(is_search_bhv)[0] as is_search_bhv,
        collect_set(first_search_keyword)[0] as first_search_keyword,
        collect_set(first_visit_source)[0] as first_visit_source,
        collect_set(source_medium)[0] as source_medium,
        collect_set(is_browse_ad)[0] as is_browse_ad,
        collect_set(first_ad_source)[0] as first_ad_source,
        collect_set(is_shared)[0] as is_shared
    FROM
        ${table_tmp_user_bhv}
    GROUP BY phone"

echo_ex "${hql}"
${HIVE} -e "${hql}"

check_success
exit 0
