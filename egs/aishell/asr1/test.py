# -*- coding: utf-8 -*-


import codecs


def get_infer_result():
    path = "/data/nipeng/TTS/espnet/egs/aishell/asr1/exp/train_sp_pytorch_train/decode_infer_decode_lm/hyp.trn"
    fout = codecs.open("infer.txt", "w")
    with codecs.open(path) as f:
        for line in f:
            t, a = line.split("(")
            text = t.replace(" ", "")
            aid = a.split("-")[0]
            aname = aid + ".wav"
            fout.write("%s\t%s\n" %(aname, text))

def test2():
    from asr_pipeline import DetectAlarmKeyword
    kw_client = DetectAlarmKeyword()
    text = "你的是傻逼滚按时发大水发老赖法萨芬"
    rst = kw_client.process(text)
    print(rst)


if __name__ == "__main__":
    print("ok")
    #get_infer_result()
    test2()
