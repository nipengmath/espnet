# -*- coding: utf-8 -*-

import sys

if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")

import os
import re
import time
import json
import redis
import codecs
import shutil
import hashlib
import tempfile
import argparse
import subprocess
from collections import defaultdict
from functools import partial
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from pydub import AudioSegment
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import CommitFailedError


def norm_aishell_data(indir, outdir):
    os.system("mkdir -p %s" %outdir)
    for path in os.listdir(indir):
        fname = path.split(".")[0]
        os.system("mkdir -p %s/data_aishell/wav/infer/%s" %(outdir, fname))
        cmd = "cp %s/%s %s/data_aishell/wav/infer/%s/." %(indir, path, outdir, fname)
        os.system("cp %s/%s %s/data_aishell/wav/infer/%s/." %(indir, path, outdir, fname))

    os.system("mkdir -p %s/data_aishell/transcript" %outdir)
    outfile = "%s/data_aishell/transcript/aishell_transcript.txt" %outdir
    fout = codecs.open(outfile, "w")
    for path in os.listdir(indir):
        fname = path.split(".")[0]
        fout.write("%s %s\n" %(fname, "哈哈"))


def gen_sp_wav_and_get_path_one(wav_temppath, audio_id, sound, item, k):
    cut_wav_name = "%s_%s_%s_%s.wav" % (audio_id, item['start'], item['end'], k)
    save_cut_path = os.path.join(wav_temppath, cut_wav_name)
    sp_wav = sound[int(item['start']):int(item['end'])]
    if sp_wav.frame_rate != 8000:
        sp_wav = sp_wav.set_frame_rate(8000)
    sp_wav.export(save_cut_path, format="wav")
    return save_cut_path


def load_hyp_file(path):
    merge_dict = {}
    with codecs.open(path) as f:
        for line in f:
            l = line.split("(")
            if len(l) == 2:
                _text, _id = l
                text = _text.strip().replace(" ", "")
                path = _id.split("-")[0] + ".wav"
                merge_dict[path] = text
    return merge_dict


def get_parser():
    parser = argparse.ArgumentParser(description='语音识别主函数参数')
    parser.add_argument("-kh", "--kafka-host",
                        default="192.168.40.22:9090,192.168.40.19:9090,192.168.40.59:9090,192.168.40.60:9090,192.168.40.61:9090",
                        required=True,
                        help="kafka host:port 集群")

    parser.add_argument("-sct", "--seg-consumer-topics",
                        default="sp_vad_topic1",
                        help="输入kafka的topic")

    parser.add_argument("-ikg", "--seg-consumer-groupid",
                        default="asr_group1",
                        help="消费kafka的groupid")

    parser.add_argument("-stm", "--session-timeout-ms",
                        default=60000,
                        type=int,
                        help="消费kafka的心跳超时时间")

    parser.add_argument("-saor", "--seg-auto-offset-reset",
                        default="largest",
                        help="重置偏移量,earliest移到最早的可用消息,latest最新的消息, 默认为largest,即是latest.消费者消费类型参数:{'smallest': 'earliest', 'largest': 'latest'}")

    parser.add_argument("-apt", "--asr-producer-topics",
                        default="asr_topic1",
                        help="输入kafka的topic")

    parser.add_argument("-fp", "--father-path",
                        default="/data/mfs/k8s/speech_pipeline/sad",
                        help="切分来源音频存放父目录")

    parser.add_argument("-wp", "--hkust-path",
                        default="/home/app/asr_pipline/kaldi/egs/hkust/s5_iqianjin",
                        help="hkust的绝对目录,即kaldi的hkust目录")

    parser.add_argument("-nj", "--num-job",
                        type=int, default=10,
                        help="espnet decode num job default 10")

    parser.add_argument("-nw", "--num-workers",
                        type=int, default=18,
                        help="multiprocess number of workers")

    parser.add_argument("-cg", "--consumer-gap",
                        type=int, default=10,
                        help="kafka consumer msg num")

    parser.add_argument("-ptm", "--poll-timeout-ms",
                        type=int, default=60000,
                        help="")

    args = parser.parse_args()
    return args


class DetectAlarmKeyword(object):
    def __init__(self):
        kws = ["狗杂种", "操你妈", "傻逼", "他妈的","你妈逼",
               "狗日的","王八蛋", "妈了个逼","婊子", "去你妈",
               "我操", "我草","贱人", "被车撞死", "搞死",
               "密码给我", "老赖","曝通讯录", "所有联系人", "不要脸",
               "去死","要不要脸", "打爆你",
        ]
        self.p_kw = re.compile("|".join(kws))

    def process(self, text):
        rst = self.p_kw.findall(text)
        return rst


class ASR(object):
    def __init__(self, kafka_servers, seg_consumer_topics, seg_consumer_groupid,
                 session_timeout_ms=60000, seg_auto_offset_reset="largest",
                 asr_producer_topics="asr_topic1", num_job=10,
                 poll_timeout_ms=60000, consumer_gap=None, num_workers=cpu_count()):
        """
        :param kafka_servers: kafka host:port
        :param seg_consumer_topics: 切分的消费者topic
        :param seg_consumer_groupid: 消费者group id
        :param session_timeout_ms: 心跳超时时间
        :param seg_auto_offset_reset: 重置偏移量, earliest移到最早的可用消息, latest最新的消息, 默认为largest,即是latest.
                                  源码定义: {'smallest': 'earliest', 'largest': 'latest'}
        :param asr_producer_topics: 语音是被的生产者topic,默认值:asr_topic1
        :param num_job: 语音识别的线程数
        """
        self.kafka_servers = kafka_servers
        self.seg_consumer_groupid = seg_consumer_groupid
        self.session_timeout_ms = session_timeout_ms
        self.seg_auto_offset_reset = seg_auto_offset_reset
        self.seg_consumer_topics = seg_consumer_topics
        self.num_job = num_job
        self.poll_timeout_ms = poll_timeout_ms
        self.consumer_gap = consumer_gap
        self.num_workers = num_workers

        self._get_from_client()

        # 语音识别结果的kafka生产者
        self.to_client = KafkaProducer(bootstrap_servers=kafka_servers,  # kafka host:port
                                       compression_type="gzip",
                                       max_request_size=1024 * 1024 * 20)
        self.asr_producer_topics = asr_producer_topics  # ASR生产者topic
        self.redis_client = redis.Redis(host='192.168.192.202', port=40029, db=0, password="Q8TYmIwQSHNFbLJ2")
        self.kw_client = DetectAlarmKeyword()

    def _get_from_client(self):
        # 消费者切分好的音频kafka消费者
        self.from_client = KafkaConsumer(bootstrap_servers=self.kafka_servers,  # kafka host:port
                                         group_id=self.seg_consumer_groupid,  # 消费者group id
                                         session_timeout_ms=self.session_timeout_ms,  # 设置心跳超时时间
                                         enable_auto_commit=False,  # 是否自动提交
                                         auto_offset_reset=self.seg_auto_offset_reset)  # 消费重置偏移量
        self.from_client.subscribe(self.seg_consumer_topics)  # 切分的消费者topic

    def check_alarm_keyword(self, merge_dict):
        rst = None
        if merge_dict:
            text = ";".join(merge_dict.values())
            kw_rsp = self.kw_client.process(text)
            if kw_rsp:
                rst = kw_rsp
        return rst

    def asr_pipline_from_kafka(self, father_path):
        """
        获取kafka的数据流，并进行识别，合并，标点，存入es
        :param father_path: 切分来源音频存放父目录
        :return:
        """
        while True:
            if not self.from_client:
                self._get_from_client()

            tp_msgs = self.from_client.poll(timeout_ms=self.poll_timeout_ms,
                                            max_records=self.consumer_gap)
            msgs = []
            for tp, _msgs in tp_msgs.items():
                msgs.extend(_msgs)

            self.batch_asr_pipline(father_path, msgs)


    def batch_asr_pipline(self, father_path, msgs):
        flag = False  # flag 是否语音识别成功并存入kafka
        wav_temppath = tempfile.mkdtemp(prefix="asr_")
        wav_normpath = wav_temppath + "_norm"

        batch_wav_lst = []
        # 所有数据
        batch_voice_data = {}
        # 包含报警关键词的数据
        batch_voice_data_imp = {}
        batch_merge_dict = None

        try:
            for msg in msgs:
                if msg is not None:
                    audio_id = json.loads(msg.value).get('audio_id', '')
                    if not self.redis_client.get('asr_espnet_' + str(audio_id)):
                        voice_data = json.loads(msg.value)
                        batch_voice_data[audio_id] = voice_data
                        if self.check_alarm_keyword(voice_data.get("merge_dict")):
                            voice_data['start_asr_espnet'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            batch_voice_data_imp[audio_id] = voice_data

            if batch_voice_data_imp:
                batch_wav_lst = self.gen_sp_wav_and_get_path_mp(father_path, wav_temppath, batch_voice_data_imp)
                norm_aishell_data(wav_temppath, wav_normpath)
                merge_dict = self._asr_cmd(wav_normpath) if batch_wav_lst else {}
                batch_merge_dict = self.split_merge_dict(merge_dict)
        except Exception as e:
           print("asr cmd error log: %s, msg: %s" % (e, ""))
        finally:
            # 删除临时音频的文件夹和语音识别结果的临时文件
            os.system("rm -rf %s" %wav_temppath)
            os.system("rm -rf %s" %wav_normpath)

        for audio_id, voice_data in batch_voice_data.items():
            try:
                if batch_merge_dict and audio_id in batch_merge_dict:
                    voice_data["merge_dict_espnet"] = batch_merge_dict[audio_id]
                    voice_data['end_asr_espnet'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    _alarm_rsp = self.check_alarm_keyword(batch_merge_dict[audio_id])
                    if _alarm_rsp:
                        voice_data["has_alarm_keyword"] = True
                        voice_data["alarm_keywords"] = json.dumps(_alarm_rsp)
                        voice_data["asr_model"] = "espnet_20191030"
                flag = self._kafka_producers(voice_data)
            except Exception as e:
                print("kafka producer error log: %s, msg: %s" % (e, ""))
        try:
            self.from_client.commit()
        except CommitFailedError:
            print('commit error!')
            for audio_id, _ in batch_voice_data.items():
                self.redis_client.set('asr_espnet_' + str(audio_id), 1, ex=24 * 3600)

        return flag

    def split_merge_dict(self, merge_dict):
        """
        拆分merge_dict
        :param merge_dict: {"A_00*_**.wav": "123",
                            "A_**_***.wav": "321",
                            "B_**_***.wav": "423"}
        """
        split_dict = defaultdict(dict)
        for wav_file, text in merge_dict.items():
            audio_id = wav_file.split("_")[0]
            split_dict[audio_id][wav_file] = text
        return split_dict

    def gen_sp_wav_and_get_path_mp(self, father_path, wav_temppath, batch_voice_data):
        """
        multiprocess generator
        step2-1: 根据msg信息获取音频，并按提前提供的音频片段开始结束时间，生成切分后的wav
        :param father_path: 音频来源目录,结果存储
        :param wav_temppath: kafka来源信息中的包含切分音频的信息
        :return: wav_lst:
        """
        p = ProcessPoolExecutor(max_workers=self.num_workers)  #不填则默认为cpu的个数
        result = []
        wav_lst = []  # wav 音频存储
        for _, voice_data in batch_voice_data.items():
            source = voice_data["source"]
            code = None if source != 'infobird' else 'pcm_s16le'
            date = voice_data["date"]
            audio_id = voice_data["audio_id"]
            wav_father_path = os.path.join(father_path, source, date)  # /data/mfs/k8s/speech_pipeline/raw/{source}/{date}
            for k, v in voice_data['segments'].items():
                if k in ["gk", "kf"] and len(v) > 0:
                    wav_name = "%s_%s.wav" % (audio_id, k)
                    raw_wav_path = os.path.join(wav_father_path, wav_name)
                    sound = AudioSegment.from_file(raw_wav_path, codec=code, format="wav")
                    for item in v:
                        # print(item, type(item))
                        obj = p.submit(partial(gen_sp_wav_and_get_path_one, wav_temppath, audio_id, sound, item, k))
                        result.append(obj)
        p.shutdown()
        res = [obj.result() for obj in result]
        return res

    def _asr_cmd(self, wav_path):
        """
        step3: 语音识别, 并获取merge的text
        :param hkust_path: hkust的绝对目录,即kaldi的hkust目录
        :param wav_path: 需识别音频存放目录
        :return:
        """
        flag = wav_path.split("/")[-1].replace("asr", "").replace("norm", "")
        decode_cmd = "./infer.sh {} {} {}".format(wav_path, flag, self.num_job)
        os.system(decode_cmd)
        decode_path = "exp/train_sp_pytorch_train/decode_infer_%s_decode_lm/hyp.trn" %flag
        merge_dict = load_hyp_file(decode_path)

        ## 删除临时目录
        data_dir = "data/infer_%s" %flag
        fbank_dir = "fbank_%s" %flag
        dump_dir = "dump/infer_%s" %flag
        decode_dir = "exp/train_sp_pytorch_train/decode_infer_%s_decode_lm" %flag
        dump_feats_dir = "exp/dump_feats/recog/infer_%s" %flag
        make_fbank_dir = "exp/make_fbank/infer_%s" %flag

        for _dir in [data_dir, fbank_dir, dump_dir, dump_feats_dir, make_fbank_dir, decode_dir]:
            os.system("rm -rf %s" %_dir)

        return merge_dict

    def _kafka_producers(self, voice_data):
        """
        step6: 将语音识别好的结果存入kafka中
        :param voice_data: 来源kafka音频结果与语音识别结果结合体,从中获取相关信息，包含识别结果
        :return:
        """
        flag = False
        try:
            audio_id = voice_data.get("audio_id", "")
            asr_model = voice_data.get("asr_model", "")
            k = self._create_id_by_input(audio_id + asr_model).encode("utf8")
            v = json.dumps(voice_data).encode("utf8")
            self.to_client.send(topic=self.asr_producer_topics, key=k, value=v)
            flag = True
        except Exception as e:
            # logger.error("error: %s, voice_data: %s" % (e, voice_data))
            print("error: %s, voice_data: %" % (e, json.dumps(voice_data, ensure_ascii=True)))
        finally:
            return flag

    def _create_id_by_input(self, id=""):
        """
        依据输入与时间生成一个唯一的md5的id
        :param x:
        :return:
        """
        x = str(id).encode("utf8")
        m = hashlib.md5(x)
        return m.hexdigest()


if __name__ == '__main__':
    args = get_parser()
    kafka_host, session_timeout_ms = args.kafka_host, args.session_timeout_ms
    seg_consumer_topics, seg_auto_offset_reset = args.seg_consumer_topics, args.seg_auto_offset_reset  # 消费切分结果的消费者参数
    seg_consumer_groupid, asr_producer_topics = args.seg_consumer_groupid, args.asr_producer_topics  # 语音识别生产者参数
    father_path, hkust_path = args.father_path, args.hkust_path  # 音频与模型相关参数
    num_job = args.num_job  # 线程数
    poll_timeout_ms = args.poll_timeout_ms
    consumer_gap = args.consumer_gap
    num_workers = args.num_workers

    # python2 asr_pipline.py --kafka-host 192.168.40.22:9090,192.168.40.19:9090,192.168.40.59:9090,192.168.40.60:9090,192.168.40.61:9090 \
    # --seg_consumer_topics sp_vad_topic1 \
    # --seg_consumer_groupid asr_group1 \
    # --seg-auto-offset-reset largest \
    # --father-path /data/mfs/k8s/speech_pipeline/sad \
    # --hkust-path /home/app/asr_pipline/kaldi/egs/hkust/s5_iqianjin

    if type(seg_consumer_topics) is str:
        seg_consumer_topics = [seg_consumer_topics, ]
    asr = ASR(kafka_servers=kafka_host, seg_consumer_topics=seg_consumer_topics, session_timeout_ms=session_timeout_ms,
              seg_consumer_groupid=seg_consumer_groupid, seg_auto_offset_reset=seg_auto_offset_reset,
              asr_producer_topics=asr_producer_topics, num_job=num_job,
              poll_timeout_ms=poll_timeout_ms, consumer_gap=consumer_gap, num_workers=num_workers)

    asr.asr_pipline_from_kafka(father_path)
