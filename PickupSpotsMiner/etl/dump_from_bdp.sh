#!/bin/bash
#################################################################################
# 备注：此脚本从bdp里面按月批量导出订单和订单扩展的坐标数据，scp至本地机器
# Target Table :
# Source Table : fo_service_order，fo_service_order_ext
# Interface Name:
# Refresh Frequency: 一次性导出
# Version Info : 1.0.0
# 修改人   修改时间       修改原因
# ------   -----------    --------------------
# lujin    20170111       创建
#################################################################################
echo "中台订单坐标批量导出（最近3个月）"
month1=`date -d "1 month ago" "+%Y%m"`
month2=`date -d "2 month ago" "+%Y%m"`
month3=`date -d "3 month ago" "+%Y%m"`
#month4=`date -d "4 month ago" "+%Y%m"`
#month5=`date -d "5 month ago" "+%Y%m"`
#month6=`date -d "6 month ago" "+%Y%m"`

dump()
{
  TMP_DIR="/tmp/user_lujin/$2"
  if [ -d $TMP_DIR ]; then
      echo "[DEBUG] clear dir $TMP_DIR"
      rm -fr $TMP_DIR
      mkdir -p $TMP_DIR
  else
      mkdir -p $TMP_DIR
  fi
  SQL="insert overwrite local directory '$TMP_DIR' row format delimited fields terminated by '\t' select $COLS from $1.$2 $EXT_WHERE"
  echo "[DEBUG] $SQL"
  hive -e "${SQL}"

  OUTFILE="${TMP_DIR}_geo_$month.tsv"
  echo "[DEBUG] result dumped to $OUTFILE"
  cat $TMP_DIR/* > $OUTFILE
  sshpass -p '123Qwe,./' scp ${OUTFILE} lujin@172.17.0.57:/home/lujin/tmp/
  rm $OUTFILE
}

declare -a month_list=(${month1} ${month2} ${month3} ${month4} ${month5} ${month6})

for month in "${month_list[@]}"; do
    echo "---------------------------------"
    WHERE=" where dt like '$month%' "
    echo "[INFO] $WHERE"

    echo "[TRACE] STEP 1. dump yc_bit.ods_service_order for geo"
    COLS="distinct dt,city,service_order_id,user_id,driver_id,start_time, end_time,start_position,start_address,end_position,end_address,expect_start_latitude,expect_start_longitude,expect_end_latitude,expect_end_longitude,start_latitude,start_longitude,end_latitude,end_longitude "
    EXT_WHERE="$WHERE and status=7 and start_latitude>0 and start_longitude>0 and expect_start_latitude>0 and expect_start_longitude>0 and passenger_name not like '%测试%'"
    dump yc_bit fo_service_order

    echo "[TRACE] STEP 2. dump yc_bit.fo_service_order_ext for geo"
    COLS="distinct dt,dest_city,service_order_id,create_order_longitude,create_order_latitude,confirm_latitude,confirm_longitude,arrive_latitude,arrive_longitude"
    EXT_WHERE="$WHERE and create_order_longitude>0 and create_order_latitude>0 and arrive_latitude>0 and arrive_longitude>0"
    dump yc_bit fo_service_order_ext
done

echo "[NOTICE] all is done!"
