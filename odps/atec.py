import jieba
import pypinyin

def seq_len_stat(t,num):
    count=0
    for i, l in enumerate(t):
        ls=l.split('\t')
        a=ls[1].decode('utf-8')
        b=ls[2].decode('utf-8')
        if len(a)>num or len(b)>num:
            count+=1
            print('{},{},{}'.format(i,len(a),len(b)))
    print(count)

def get_pinyin_dict():
    """"
    使用pinyin库将atec初赛训练数据中的所有词转换为pinyin , 并相互映射，得到这对于拼音和词的字典，
    传到复赛的线上使用
    """
    file=open("odps/data/atec_nlp_sim_train_all.csv",'r',encoding="utf-8")
    stopfile=open('odps/data/stop_words.txt','r',encoding='utf-8')
    jieba.load_userdict('odps/data/words.txt')
    stopwords=set()
    for word in stopfile.readlines()[1:]:
        stopwords.add(word.strip())
    lines = file.readlines()
    sents=[]
    for line in lines:
        _,sent1,sent2,_ = line.split('\t')
        sent1=jieba.cut(sent1)
        sent2=jieba.cut(sent2)
        sents.extend(list(sent1)+list(sent2))
    words=set(sents)
    words=list(words-stopwords)
    words_pinyin=[]
    for w in words:
        words_pinyin.append(''.join([i[0] for i in pypinyin.pinyin(w,style=pypinyin.NORMAL)]))
    pinyin_dict=dict(zip(words,words_pinyin))
    dict_file=open("odps/data/pinyin_dict.txt",'w',encoding="utf-8")
    dict_file.write('word|pinyin\n')
    for word,py in pinyin_dict.items():
        dict_file.write('{}|{}\n'.format(word,py))
    dict_file.close()
    return pinyin_dict





if __name__ == '__main__':
    get_pinyin_dict()