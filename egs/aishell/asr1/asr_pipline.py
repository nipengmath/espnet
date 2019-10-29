# -*- coding: utf-8 -*-

import sys

if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")

import os
import time
import json
import redis
import jieba
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
from elasticsearch import Elasticsearch, helpers


def norm_aishell_data(indir):
    outdir = indir + "_norm"
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
    return outdir


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
                        help="hkust num job default 10")

    parser.add_argument("-nw", "--num-workers",
                        type=int, default=18,
                        help="multiprocess number of workers")

    parser.add_argument("-cg", "--consumer-gap",
                        type=int, default=10,
                        help="kafka consumer msg num")

    parser.add_argument("-ptm", "--poll-timeout-ms",
                        type=int, default=60000,
                        help="")

    parser.add_argument('--is-comp', help='1为补数,0为不进入补数筛选逻辑', default=0, type=int)
    args = parser.parse_args()
    return args


class ASR(object):
    def __init__(self, kafka_servers, seg_consumer_topics, seg_consumer_groupid,
                 session_timeout_ms=60000, seg_auto_offset_reset="largest",
                 asr_producer_topics="asr_topic1", num_job=10, is_comp=0,
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
        :param is_comp: 是否为补数逻辑,是的话会进入时间判断和es数据判断
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

        self.is_comp = is_comp
        if is_comp:
            self.comp_num = 0
            self.prop = {"HOST1": "192.168.40.37",
                         "HOST2": "192.168.40.38",
                         "HOST3": "192.168.40.39",
                         "PORT": "9200",
                         "DOC_TYPE": "kf_infobird_call"}

            self.es = Elasticsearch(hosts=[{'host': self.prop['HOST1'], 'port': self.prop['PORT']},
                                           {'host': self.prop['HOST2'], 'port': self.prop['PORT']},
                                           {'host': self.prop['HOST3'], 'port': self.prop['PORT']}])

    def _get_from_client(self):
        # 消费者切分好的音频kafka消费者
        self.from_client = KafkaConsumer(bootstrap_servers=self.kafka_servers,  # kafka host:port
                                         group_id=self.seg_consumer_groupid,  # 消费者group id
                                         session_timeout_ms=self.session_timeout_ms,  # 设置心跳超时时间
                                         enable_auto_commit=False,  # 是否自动提交
                                         auto_offset_reset=self.seg_auto_offset_reset)  # 消费重置偏移量
        self.from_client.subscribe(self.seg_consumer_topics)  # 切分的消费者topic

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
        """
        单个kafka消息消费:
        1)从kafka消息中提取value信息;
        2)根据msg.value信息从切分好的数据源复制到目标位置;
        3)进行语音识别,放进merge_dict里;
        4)写入kafka新的topic;
        7)删除临时音频的文件夹和语音识别结果的临时文件.
        :param father_path: 切分来源音频存放父目录;
        :param msg: 从kafka获取的完整消息;
        :return:
        """
        flag = False  # flag 是否语音识别成功并存入kafka
        wav_temppath = tempfile.mkdtemp(prefix="asr_")
        batch_wav_lst = []
        batch_voice_data = {}
        batch_merge_dict = None

        try:
            for msg in msgs:
                if msg is not None:
                    audio_id = json.loads(msg.value).get('audio_id', '')
                    if not redis_client.get('asr_espnet_' + str(audio_id)):
                        # step1: 从kafka消息中提取value信息
                        voice_data = json.loads(msg.value)
                        voice_data['start_asr_espnet'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        batch_voice_data[audio_id] = voice_data  # 后面继续往 voice_data 追加数据
            # step2：根据msg.value信息获取音频，并按提前提供的音频片段开始结束时间，生成切分后的wav
            batch_wav_lst = self.gen_sp_wav_and_get_path_mp(father_path, wav_temppath, batch_voice_data)
            # step3: 语音识别, 并获取merge的text
            # batch results
            wav_normpath = norm_aishell_data(wav_temppath)
            merge_dict = self._asr_cmd(wav_normpath) if batch_wav_lst else {}
            batch_merge_dict = self.split_merge_dict(merge_dict)
        except Exception as e:
            print("bebefore commit to kafka error log: %s, msg: %s" % (e, ""))
        finally:
            # step7: 删除临时音频的文件夹和语音识别结果的临时文件
            shutil.rmtree(wav_temppath)
            shutil.rmtree(wav_normpath)

        for audio_id, voice_data in batch_voice_data.items():
            # step4: 写入kafka新的topic
            try:
                voice_data["merge_dict_espnet"] = batch_merge_dict[audio_id]
                voice_data['end_asr_espnet'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                flag = self._kafka_producers(voice_data) if voice_data.get("merge_dict", {}) else False
            except Exception as e:
                print("before commit to kafka error log: %s, msg: %s" % (e, ""))
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

    def filter_conditions(self, voice_data):
        """
        筛选出不需要补数的数据,并抛出异常;
        :param voice_data:
        :return:
        """
        end_sad = voice_data.get("end_sad", "0000-00-00 00:00:00")
        audio_id = voice_data.get("audio_id", None)
        body = {
            "query": {"bool": {"must": [{"term": {"audio_id.keyword": audio_id}}], "must_not": [], "should": []}},
            "from": 0, "size": 10, "sort": [], "aggs": {}}
        if end_sad < "2019-07-11 12:00:00" or end_sad > "2019-07-11 21:00:00":
            raise RuntimeError('This one\'s end_sad not in time window !')
        query = self.es.search(index="asr_text",
                               doc_type=self.prop['DOC_TYPE'],
                               scroll='5m',
                               timeout='30s',
                               body=body)
        results = query['hits']['hits']  # es查询出的结果第一页
        total = query['hits']['total']  # es查询出的结果总量
        print("audio_id:", audio_id, "search num:", total, ",end_sad", end_sad)
        if total > 0:
            line = results[0]["_source"]
            if line.get("cut_text"):
                raise RuntimeError('This id already exists and already asr !')
        else:
            segments = voice_data.get("segments", {})
            for k, v in segments.items():
                if k in ["gk", "kf"] and len(v) > 0:
                    break
            else:
                raise RuntimeError('This data segments is null !')
        self.comp_num += 1

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
    is_comp = args.is_comp  # 是否走补数过滤流程
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
              asr_producer_topics=asr_producer_topics, num_job=num_job, is_comp=is_comp,
              poll_timeout_ms=poll_timeout_ms, consumer_gap=consumer_gap, num_workers=num_workers)

    asr.asr_pipline_from_kafka(father_path)