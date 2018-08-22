def get_syns_ngram(syns_ngram_list):
    syns = [x.strip().split(',') for x in syns_ngram_list]
    syn_set_list=[]
    for syn in syns:
        syn_set=set()
        for s in syn:
            s=' '.join(sorted(s.strip().split()))
            syn_set.add(s)
        syn_set_list.append(syn_set)
    return syn_set_list


def ngram_merge(syn_set_list):
    merge_list=list()
    for syn_set in syn_set_list:
        syn_list=list(syn_set)
        list_len=len(syn_list)
        if list_len<=1:
            continue
        for i in range(list_len-1):
            pairs=[' '.join(sorted([syn_list[i],x])) for x in syn_list[i+1:]]
            merge_list.extend(pairs)
    merge_set = set(merge_list)
    return merge_set

def judge(x,y):
    x=x.strip().split()
    y=y.strip().split()
    x_set=set(x)
    y_set=set(y)
    for i in x:
        if i in y_set:
            x_set.remove(i)
            y_set.remove(i)
    x_str=' '.join(sorted(x_set))
    y_str=' '.join(sorted(y_set))
    pair_str = ' '.join([x_str,y_str] if x_str<y_str else [y_str,x_str])
    if pair_str in ng.value:
        return 1
    return 0







