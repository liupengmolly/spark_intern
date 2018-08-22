import numpy as np
from scipy.spatial.distance import cosine
import time
from scipy.sparse import csr_matrix
from numpy.linalg import norm

def cosine_array(indices_a,indices_b,values_a,values_b):
    """
    计算稀疏序列的余弦相似度（优化版）
    :param indices_a:
    :param indices_b:
    :param values_a:
    :param values_b:
    :return:
    """
    result=0.0
    i_a,i_b,len_a,len_b=0,0,len(indices_a),len(indices_b)
    while i_a<len_a and i_b<len_b:
        ind_a,ind_b=indices_a[i_a],indices_b[i_b]
        if ind_a==ind_b:
            result+=values_a[i_a]*values_b[i_b]
            i_a+=1;i_b+=1
        elif ind_a>ind_b:
            i_b+=1
        else:
            i_a+=1
    result=result/(norm(values_a)*norm(values_b))
    print(result)

def raw_cosine_array(indices_a,indices_b,values_a,values_b):
    vec_a, vec_b = np.zeros(100000), np.zeros(100000)
    vec_a[indices_a], vec_b[indices_b] = values_a, values_b
    result=1.0 - cosine(vec_a, vec_b)
    print(result)

def cosine_sparse(indices_a,indices_b,values_a,values_b):
    """
    计算稀疏序列的余弦相似度（但实际是按照普通序列计算，计算有浪费）
    :param indices_a:
    :param indices_b:
    :param values_a:
    :param values_b:
    :return:
    """
    row_a,row_b=np.array(indices_a),np.array(indices_b)
    col_a,col_b=np.zeros(row_a.size),np.zeros(row_b.size)
    data_a,data_b=np.array(values_a),np.array(values_b)
    sparse_a,sparse_b=csr_matrix((data_a,(row_a,col_a)),shape=(100000,1)),\
                      csr_matrix((data_b,(row_b,col_b)),shape=(100000,1))
    inner=sparse_a.transpose().dot(sparse_b).sum()
    sqrt_a=np.sqrt(sparse_a.transpose().dot(sparse_a).sum())
    sqrt_b=np.sqrt(sparse_b.transpose().dot(sparse_b).sum())
    result=inner/(sqrt_a*sqrt_b)
    print(result)

def cal_sim(x, y):
    a, b = x.split('_'), y.split("_")
    len_a, len_b = len(a), len(b)
    time_one=time.clock()
    indices_a, indices_b = list(map(int, a[:int(len_a/2)])), \
                           list(map(int, b[:int(len_b/2)]))
    values_a, values_b = list(map(float, a[int(len_a / 2):])), \
                         list(map(float, b[int(len_b /2):]))
    # indices_a, indices_b = [int(i) for i in a[:int(len_a/2)]], [int(i) for i in  b[:int(len_b/2)]]
    # values_a, values_b = [float(i) for i in a[int(len_a / 2):]], [float(i) for i in b[int(len_b /2):]]

    time_two=time.clock()
    cosine_array(indices_a,indices_b,values_a,values_b)

    time_three=time.clock()
    raw_cosine_array(indices_a,indices_b,values_a,values_b)
    # cosine_sparse(indices_a,indices_b,values_a,values_b)

    time_four=time.clock()
    return time_two-time_one,time_three-time_two,time_four-time_three

def test(n):
    x='3_5_13_84_138_900_3221_9000_2.32_99.4_94.4252_89.2_2.424_5.424_2.425_8.425'
    y='5_9_13_34_94_913_1425_5800_13.1_425.522_400.56_900.31_2.55_5.42_6.53_95.33'
    stage1,stage2,stage3=0.0,0.0,0.0
    for i in range(n):
        a,b,c=cal_sim(x,y)
        stage1+=a
        stage2+=b
        stage3+=c
    print(stage1,stage2,stage3)

def sim(pairs):
    """
    计算给定pairs的余弦相似度
    :param pairs:
    :return:
    """
    from pyspark.sql.types import FloatType
    from pyspark.sql.functions import col,udf
    from numpy.linalg import norm
    def cal_sim(x,y):
        a,b=x.split('_'),y.split("_")
        len_a,len_b=len(a),len(b)
        indices_a,indices_b=list(map(int,a[:len_a/2])),list(map(int,b[:len_b/2]))
        values_a,values_b=list(map(float,a[len_a/2:])),list(map(float,b[len_b/2:]))
        result=0.0
        i_a,i_b,len_inda,len_indb=0,0,len(indices_a),len(indices_b)
        while i_a<len_inda and i_b<len_indb:
            ind_a,ind_b=indices_a[i_a],indices_b[i_b]
            if ind_a==ind_b:
                result+=values_a[i_a]*values_b[i_b]
                i_a+=1;i_b+=1
            elif ind_a>ind_b:
                i_b+=1
            else:
                i_a+=1
        return float(result/(norm(values_a)*norm(values_b)))
    pairs=pairs.where(pairs.f1!='')
    pairs=pairs.where(pairs.f2!='')
    sim_udf=udf(cal_sim,FloatType())
    result=pairs.select("_1","_2",sim_udf(pairs.f1,pairs.f2))
    result=result.withColumnRenamed("cal_sim(f1, f2)",'sim')
    result.write.parquet("tfidf_pairs_sim_0428")

def top_10(x):
    len_citys=len(x[1])
    if len_citys==10:
        return x[1]
    else:
        citys=x[1]
        nocitys=x[0]
        for i in x[0]:
            if i not in citys:
                citys.append(i)
        return citys

def get_result(sample,cmp,id_name,op,scope,kw):
    """
    从sample中的推荐公司字段（recmd_cmp）中抽取top10，然后对每条数据标记其来源，其法为与原始的
    来源数据op(opponent),scope,kw(scope key word)join, 然后标记
    :param sample:id
    :param cmp:c_idx,c_id,recmd_type,recmd_cmp
    :param id_name:_1,name
    :param op:_1,_2
    :param scope:_1,_2

    :return:
    """
    from pyspark.sql.functions import udf,col
    from pyspark.sql.types import ArrayType,StructType,StringType,IntegerType,DoubleType,BooleanType,StructField
    sample=sample.join(id_name,sample.id==id_name._1)[['id','name']]
    sample=sample.join(cmp,sample.id==cmp.c_id)[['id','name','recmd_cmp']]
    top10_udf=udf(lambda x:sorted(x,key=lambda x:x[2],reverse=True)[:10],
                  ArrayType(StructType([StructField('recmd_id',StringType()),
                                        StructField('recmd_name',StringType()),
                                        StructField('score',StringType())])))
    sample=sample.withColumn('top10',top10_udf(col('recmd_cmp')))
    sample=sample[['id','name','top10']]
    results=sample.rdd.flatMap(lambda x:[(x['id'],x['name'],t[0],t[1],t[2])
                                         for t in x['top10']]).toDF()
    results=results.withColumnRenamed("_1",'id1')
    results=results.withColumnRenamed("_2",'name1')
    results=results.withColumnRenamed("_3",'id2')
    results=results.withColumnRenamed("_4",'name2')

    cols=results.columns
    op_r=results.join(op,(results.id1==op._1)&(results.id2==op._2))[cols]
    rev_op_r=results.join(op,(results.id2==op._1)&(results.id1==op._2))[cols]
    scope_r=results.join(scope,(results.id1==scope._1)&(results.id2==scope._2))[cols]
    rev_scope_r=results.join(scope,(results.id2==scope._1)&(results.id1==scope._2))[cols]
    kw_r=results.join(kw,(results.id1==kw._1)&(results.id2==kw._2))[cols]
    rev_kw_r=results.join(kw,(results.id2==kw._1)&(results.id1==kw._2))[cols]

    ops=op_r.union(rev_op_r).distinct()
    scopes=scope_r.union(rev_scope_r).distinct()
    kws=kw_r.union(rev_kw_r).distinct()

    op_udf=udf(lambda x:'opponent',StringType())
    scope_udf=udf(lambda x:'scope',StringType())
    kw_udf=udf(lambda x:'scope_key_words',StringType())

    ops=ops.withColumn('_6',op_udf(ops.id1))
    scopes=scopes.withColumn('_6',scope_udf(scopes.id1))
    kws=kws.withColumn('_6',kw_udf(kws.id1))

    results=ops.union(scopes)
    results=results.union(kws)
    results.write.parquet('/data/peng_liu/sim_company/0523_random_samples')


if __name__=='__main__':
    test(10)