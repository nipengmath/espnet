#!/usr/bin/bash
#vim /etc/hosts
## Entries added by HostAliases.
#192.168.176.181	daasoffline1.kafka.dc.puhuifinance.com
#192.168.176.182	daasoffline2.kafka.dc.puhuifinance.com
#192.168.176.183	daasoffline3.kafka.dc.puhuifinance.com
#192.168.176.184	daasoffline4.kafka.dc.puhuifinance.com
#192.168.176.185	daasoffline5.kafka.dc.puhuifinance.com

# test
# 测试需要在代码内修改生产则kafka host
python3 asr_pipline.py \
  --kafka-host daasoffline1.kafka.dc.puhuifinance.com:6667,daasoffline2.kafka.dc.puhuifinance.com:6667,daasoffline3.kafka.dc.puhuifinance.com:6667,daasoffline4.kafka.dc.puhuifinance.com:6667,daasoffline5.kafka.dc.puhuifinance.com:6667 \
  --seg-consumer-topics sp_asr_topic \
  --seg-consumer-groupid sp_sad_asr_group_np_20191028_v3 \
  --session-timeout-ms 30000 \
  --seg-auto-offset-reset smallest \
  --asr-producer-topics asr_topic1_t1 \
  --father-path /data/mfs/k8s/speech_pipeline/raw \
  --hkust-path /home/app/hkust/kaldi/egs/hkust/s5_daihou \
  --num-job 10
#  --is-comp 1


## online
#python2 -u asr_pipline.py --kafka-host daasoffline1.kafka.dc.puhuifinance.com:6667,daasoffline2.kafka.dc.puhuifinance.com:6667,daasoffline3.kafka.dc.puhuifinance.com:6667,daasoffline4.kafka.dc.puhuifinance.com:6667,daasoffline5.kafka.dc.puhuifinance.com:6667 \
#  --seg-consumer-topics sp_sad_topic \
#  --seg-consumer-groupid sp_sad_asr_group \
#  --session-timeout-ms 60000 \
#  --seg-auto-offset-reset largest \
#  --asr-producer-topics sp_asr_topic \
#  --father-path /data/mfs/k8s/speech_pipeline/sad \
#  --hkust-path /home/app/asr_pipline/kaldi/egs/hkust/s5_daihou \
#  --num-job 10
