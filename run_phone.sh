#!/bin/sh

#source /home/work/.bashrc
#source /home/work/.bash_profile
#git pull
#mvn clean package -U
#set
#queue="miui_recommendation"
#queue="root.default"
#queue="root.service.miui_group.miui_ad"
queue="root.production.miui_group.miui_ad.queue_1"
num_executor="200"
master="yarn-cluster"
#master="local[*]"

#HADOOP_HOME="/home/work/tools/infra-client"
SPARK_HOME="/home/work/tools/infra-client"

#JAR path
JAR_PATH="/home/work/wwxu/exp/jars/feature_extractor-1.0-SNAPSHOT.jar"

#input path
date=`date -d "$1 days ago" +%Y%m%d`
hist_date=`date -d "$1 days ago" +%Y_%m_%d`
year=`date -d "$1 days ago" +%Y`
month=`date -d "$1 days ago" +%m`
day=`date -d "$1 days ago" +%d`
echo $year, $month, $day, $hist_date, $date

#adInfoPath=/user/h_miui_ad/feed_ads/yidian/data/ad_info_target
#adInfoPath=/user/h_miui_ad/feed_ads/yidian/data/ad_info_target_tag
adInfoPath=/user/h_miui_ad/ad_prediction_service_corpus/ad_corpus/ad_info_target_tag_emiv2
userInfoPath=/user/h_data_platform/platform/miuiads/ml_dau_profile/date=${date}/
historicalInfoPath=/user/h_miui_ad/ad_prediction_service_corpus/history_corpus/${hist_date}*
adEventPath=/user/h_data_platform/platform/miuiads/ad_historical_event/date=${date}/
adLogV2Path=/user/h_scribe/miuiads/miuiads_ad_log_v2/year=${year}/month=${month}/day=${day}

while [ 1 == 1 ]
do
    hadoop --cluster c3prc-hadoop fs -test -e /user/h_data_platform/platform/miuiads/ml_dau_profile/date=${date}/
    if [ $? == 0 ]
    then
        break
    fi
    echo "File /user/h_data_platform/platform/miuiads/ml_dau_profile/date=${date}/ not exist, wait"
    sleep 60
done

while [ 1 == 1 ]
do
    hadoop --cluster c3prc-hadoop fs -test -e /user/h_data_platform/platform/miuiads/ad_historical_event/date=${date}/
    if [ $? == 0 ]
    then
        break
    fi
    echo "File /user/h_data_platform/platform/miuiads/ad_historical_event/date=${date}/ not exist, wait"
    sleep 60
done

#output path
outputfiles=/user/h_miui_ad/wwxu/exp_base/phone_context/date=${date}

sampleRate=1

hadoop --cluster c3prc-hadoop fs -rmr ${outputfiles}*

#main class
class=Job.PhoneVideoWithClient

echo "submit spark"
spark-submit \
    --cluster c3prc-hadoop \
    --class $class \
    --master $master \
    --num-executors $num_executor \
    --queue $queue \
    --driver-memory 6g \
    --executor-memory 4g \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=20 \
    --conf spark.dynamicAllocation.maxExecutors=400 \
    --conf spark.yarn.driver.memoryOverhead=896 \
    --conf spark.memory.fraction=0.3 \
    $JAR_PATH $adInfoPath $userInfoPath $historicalInfoPath $adEventPath $adLogV2Path $outputfiles $sampleRate

if [ $? == 0 ]
then
#    echo "PhoneVideoWithClient Feature Extractor Done" | mail -s "PhoneVideoWithClient Feature Extractor Done" "wuhonggang@xiaomi.com,yanming@xiaomi.com"
    echo "PhoneVideoWithClient Feature Extractor Done" | mail -s "PhoneVideoWithClient Feature Extractor Done" "xuwenwen@xiaomi.com"
    exit 0
fi
#echo "PhoneVideoWithClient Feature Extractor Failed" | mail -s "PhoneVideoWithClient Feature Extractor Failed" "wuhonggang@xiaomi.com,yanming@xiaomi.com"
echo "PhoneVideoWithClient Feature Extractor Failed" | mail -s "PhoneVideoWithClient Feature Extractor Failed" "xuwenwen@xiaomi.com"

