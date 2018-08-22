"""
首先说明抽取主营公司业务模型训练数据的规则：
    抽取所有只有一个业务的公司名-主营业务对
理论上认为只有一个公司业务的即为该公司的主营业务，但肯定包含较多的噪声，所以以下规则筛选噪声

规则一：company_name与normalized_scope必须要有一个字相同
规则二：因为是通过公司名去判断核心业务，所以允许公司名中具体的业务对应scope中
        更抽象的业务，但是不允许公司名中抽象的业务对应具体的业务，于是规则的
        实现为：对公司名中的具体业务有一个对应scope中更抽象业务的map（弥补规则一
        的缺失），从而判断map的值是否在公司业务中

"""

import math
def most_present_char(name_chars_list):
    """
    得到公司名中最常出现的词
    :param name_chars_list:
    :return:mosts
    """
    word_dict={}
    for name_chars in name_chars_list:
        for c in name_chars:
            word_dict[c]=word_dict.get(c,0)+1
    words_count=sorted(word_dict.items(),key=lambda x:x[1],reverse=True)
    return words_count

def top_words(wrap_arrays,len_group):
    """
    对于包含公司数大于500的经营范围，求出其所属的公司名中最常出现的词
    :param wrap_arrays:
    :param len_group:
    :return:
    """
    words_count={}
    for array in wrap_arrays:
        for word in array:
             words_count[word]=words_count.get(word,0)+1
    words=sorted(words_count.items(),key=lambda x:x[1],reverse=True)
    return [w[0] for w in words[:math.log(len_group)]]

def filter_words(words, mosts, address):
    """
    利用函数most_persent_char得到的结果Mosts,以及规则一，过滤掉top words中无效的词
    :param words:
    :param mosts:
    :param address:
    :return:
    """
    new_words = []
    for w in words:
        if w not in mosts:
            if w[-1] not in address:
                new_words.append(w)
    return new_words

def present_most_words(most_words):
    for i,w in enumerate(most_words):
        print(str(i)+w)

def del_most_words(l,most_words):
    for i in l:
        del(most_words[i])
    return most_words



"""根据top words生成规则2的map,即字典，键为scope,值为公司名中的top_words"""
map_dict={}
for i,s in enumerate(scope2):
    words=top_words2[i]['filter_words']
    if len(words)==0:
        continue
    words_dict=dict(zip(words,[1 for j in range(len(words))]))
    map_dict[s['_2']]=words_dict


def judge(name, scope, name_words):
    """利用规则1，规则2判断每一个name，scope对是否为正例"""
    for c in name:
        if c in scope:
            if c > '9' or c < '0':
                return True
    if scope in map_dict:
        scope_dict = map_dict[scope]
    for w in name_words:
        if w in scope_dict:
            return True
    return False

"""
下面是反例的生成函数，较为简单，生成规则为：
规则1， 对明显不同scope的group之间随机交叉组合，从map中识别选取不同的scope group（依据：1 没有相同的字 2 top words没有相同）
规则2， 在map_dict包含的scope（groups>500）和100<groups<500的scope匹配（依据：scope中没有相同的字）
规则3， 在groups数量小于100的scope之间配对
规则1 约占1亿条，规则2约占0.4亿, 总共约1.4亿条

"""

def contain_same_chars(a,b):
    for c in a:
        if c in b and (c not in mosts):
            return True
    return False

def contain_same_words(a,b):
    for w in a:
        if w in b:
            return True
    return False

def get_rule1_pairs(map_dict):
    """
    获取scope包含于map_dict中的随机配对，对应于rule1
    :param map_dict:
    :return:
    """
    import random
    pairs=[]
    scopes=map_dict.keys()
    for s in scopes:
        cands=random.sample(scopes,100)
        for c in cands:
            if not contain_same_chars(s,c):
                groups_s,group_c=map_dict[s],map_dict[c]
                if not contain_same_words(groups_s,group_c):
                    pairs.append((s,c))
    pairs=list(set([tuple(sorted(p)) for p in pairs]))
    return pairs

def get_rule2_pairs(scopes1,scopes2):
    """
    获取scope包含于map_dict与scope对应group数小于500（但大于100）的随机配对，对应于规则2
    :param scopes1:
    :param scopes2:
    :return:
    """
    import random
    pairs=[]
    for s in scopes1:
        cands=random.sample(scopes2,1000)
        for c in cands:
            if not contain_same_chars(s,c):
                pairs.append((s,c))
    return pairs

"""
由于scopes太大不能collect到本地，用另一种方法实现
由于udf不能传参数scopes1,所以用rdd的map实现
"""
def sample_scopes1(x,scopes1):
    import random
    cands=random.sample(scopes1,10)
    random.shuffle(cands)
    for c in cands:
        c=c['_2']
        if not contain_same_chars(x,c):
            return c
    #最后返回的''方便进行过滤
    return (x,'')

def get_rule3_pairs(scopes):
    """
    获取scope对应group数小于100的随机配对，对应于规则3
    :param scopes:
    :return:
    """
    import random
    pairs=[]
    for s in scopes:
        s=s['_2']
        cands=random.sample(scopes,10)
        random.shuffle(cands)
        for c in cands:
            c=c['_2']
            if not contain_same_chars(s,c):
                pairs.append((s,c))
                break
    pairs=list(set([tuple(sorted(p)) for p in pairs]))
    return pairs


import random
def pair1_match(s1,s2,names1,names2):
    """
    对rule1选取出来的scope和对应的公司名列表配对展开（flatten）
    :param s1:
    :param s2:
    :param names1:
    :param names2:
    :return:
    """
    pairs=[]
    s1_cands=random.sample(names2,100)
    s2_cands=random.sample(names1,100)
    pairs.extend([(name,s2) for name in s2_cands])
    pairs.extend([(name,s1) for name in s1_cands])
    return pairs

def pair2_match(s1,s2,names1,names2):
    pairs=[]
    len1=len(names1)
    len2=len(names2)
    if len2>20:
        s1_cands=random.sample(names2,20)
    else:
        s1_cands=names2
    if len1>20:
        s2_cands=random.sample(names1,20)
    else:
        s2_cands=names1
    pairs.extend([(name, s2) for name in s2_cands])
    pairs.extend([(name, s1) for name in s1_cands])
    return pairs

def pair3_match(s1,s2,names1,names2):
    import random
    pairs=[]
    len1=len(names1)
    len2=len(names2)
    if len2>50:
        s1_cands=random.sample(names2,50)
    else:
        s1_cands=names2
    if len1>50:
        s2_cands=random.sample(names1,50)
    else:
        s2_cands=names1
    pairs.extend([(name, s2) for name in s2_cands])
    pairs.extend([(name, s1) for name in s1_cands])
    return pairs

def write_sample(num,spark):
    import pickle
    if num>5000000:
        return ValueError("the maxim value of argument num is 5000000,the current"
                          "value of num is {0}".format(str(num)))
    pos=spark.read.parquet('/data/peng_liu/core_business/0510_valid_name_scope')[['_1','_2']]
    neg1=spark.read.parquet('/data/peng_liu//core_business/0510_negative_name_scope_1')
    neg2=spark.read.parquet('/data/peng_liu//core_business/0528_negative_name_scope_2')
    neg3=spark.read.parquet('/data/peng_liu//core_business/0528_negative_name_scope_3')
    pos_count=pos.count()
    neg1_count=neg1.count()
    neg2_count=neg2.count()
    neg3_count=neg3.count()
    sp_pos_rate=float(num)/pos_count
    sp_neg1_rate=float((0.4*num)/neg1_count)
    sp_neg2_rate=float((0.3*num)/neg2_count)
    sp_neg3_rate=float((0.3*num)/neg3_count)
    sp_pos=pos.sample(False,sp_pos_rate,123)
    sp_neg1=neg1.sample(False,sp_neg1_rate,234)
    sp_neg2=neg2.sample(False,sp_neg2_rate,345)
    sp_neg3=neg3.sample(False,sp_neg3_rate,456)
    sp_neg=sp_neg1.union(sp_neg2)
    sp_neg=sp_neg.union(sp_neg3)
    sp_pos_list=sp_pos.collect()
    print(len(sp_pos_list))
    sp_neg_list=sp_neg.collect()
    print(len(sp_neg_list))
    sp_pos_list=[(r['_1'].encode('utf-8'),r['_2'].encode('utf-8')) for r in sp_pos_list]
    sp_neg_list=[(r['_1'].encode('utf-8'),r['_2'].encode('utf-8')) for r in sp_neg_list]
    with open('/data/peng_liu/data/core_business/sp_pos_list_{0}.pickle'.format(str(num)),'w') \
        as pos_file:
        pickle.dump(sp_pos_list,pos_file)
    with open('/data/peng_liu/data/core_business/sp_neg_list_{0}.pickle'.format(str(num)),'w') \
            as neg_file:
        pickle.dump(sp_neg_list,neg_file)
    print("Done")

def read_sample(num):
    import pickle
    with open('sp_pos_list_{0}.pickle'.format(str(num)),'r') as pos_file:
        pos=pickle.load(pos_file)
    with open('sp_neg_list_{0}.pickle'.format(str(num)),'r') as neg_file:
        neg=pickle.load(neg_file)
    return pos,neg

def sort_score(x):
    x=[(i,j,float(k)) for i,j,k in x]
    x=sorted(x,key=lambda k:k[2],reverse=True)
    return x[:10]


def get_most(valid):
    """
    extract the most presented chars which is both in name and scope

    :param valid:
        df[['_1','_2'.'name_words','scope_words','if_valid']]
        _1:name
        _2:scope
        name_words:splited name
        scope_words:splited_scope
        if_valid:
    :return:
    """
    from pyspark.sql.functions import col,udf
    from pyspark.sql.types import StringType
    def extract_same(x,y):
        s=''
        for i in x:
            if i in y:
                s+=i
        return s
    extract_same_udf=udf(extract_same,StringType())
    same_chars=valid.select(extract_same_udf(valid._1,valid._2).alias('same_char')).collect()
    char_dict={}
    for chars in same_chars:
        for c in chars['same_char']:
            char_dict[c]=char_dict.get(c,0)+1
    char_list=sorted(char_dict.items(),key=lambda x:x[1],reverse=True)
    return char_list

def filter_negative_pos(str1,str2):
    same_chars=[]
    for c in str1:
        if c in str2:
            same_chars.append(c)
    len1=len(same_chars)
    for c  in same_chars:
        if c in mosts:
            same_chars.remove(c)
    len2=len(same_chars)
    if len1>0 and len2==0:
        return False
    return True

def clean_data(num):
    """
    deal with the csv file about the similairty result of name_scope,
    1 cut the last char `\n` of name
    2 cut the first line
    :return:
    """
    import pandas as pd
    for i in range(num,num+4):
        path='/DeepLearning/peng_liu/old_result/result_part{}'.format(i)
        df=pd.read_csv(path,error_bad_lines=False,warn_bad_lines=False,header=0)
        df['2']=df['2'].str[:-1]
        df.iloc[1:].to_csv('result{}'.format(i),index=False,header=False)



