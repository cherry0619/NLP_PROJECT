'''
计算一个句子的困惑度。中心思想是隐马尔可夫。
 p(wn|wn-1,.....w3,w2,w1)
但存在的问题是：链路过长，计算量过大。所以会计算前几个词语。
一般是 bi-gram ；tri-gram
我们按照单字形式进行计算
为了节约内存，我们将单字全部用数字代替，维护一个字典
仅计算bi-gram
文件遍历，不全部加载
'''


import pandas as pd
from collections import Counter
import json
from tqdm import tqdm


# ------------------------------------------------------------------------------------------------------------
img_path ='your file path'
df =pd.read_csv(img_path)
# ------------------------------------------------------------------------------------------------------------




class DataReader():

    def __init__(self):
        pass

    def reade_txt(self,file_path,remove_space=True):
        with open(file_path) as f:
            text_list =f.readlines()
        text_list = [i.strip('\n') for i in text_list]
        if remove_space:
            text_list =[i.replace(' ','') for i in text_list]
        return text_list


    def read_doc(self,file_path):
        pass

    def read_json(self,file_path):
        with open(file_path,'w') as f:
            res_json =json.load(f)
        return res_json



class CountFn():
    def __init__(self):
        pass

    def word_counts(self,words_list):
        # 计算word中次数的
        res_count =Counter(words_list)
        return res_count

    def word_write(self,word_counts,save_path):
        with open(save_path, 'w') as f:
            for item in word_counts.items():
                write_str = ' '.join([item[0],str(item[1])])
                f.write(write_str)
                f.write('\n')





# ------------------------------------------------------------------------------------------------------------
files_list =df[df['format']=='txt']['abs_path']
# files_list =files_list[0:100]

Data_Reader =DataReader()
Count_fn=CountFn()


# ------------------------------------------------------------------------------------------------------------
print('count by files ~~~~')
root_dir ='/hot-data-ceph/data/NLP_PROJECT/'
save_file_list =[]

num = 0

for file_path in tqdm(files_list):
    text_list =Data_Reader.reade_txt(file_path)
    single_char_list=[]
    bi_chars_list =[]

    for line in text_list:
        single_char_list =list(line)
        bi_char_list =[line[i:i+2] for i in range(len(line)-2)]
        single_char_list.extend(single_char_list)
        bi_chars_list.extend(bi_char_list)

    # 统计每一个文件中的单字出现的次数，两个字出现的次数
    single_char_count =Count_fn.word_counts(single_char_list)
    bi_char_count =Count_fn.word_counts(bi_chars_list)
    # 然后把这次数据保存起来
    file_path =root_dir +'{}.txt'.format(num)
    save_file_list.append(file_path)
    single_char_path =file_path.replace('txt','single_json')
    bi_chars_path =file_path.replace('txt','bi_json')
    Count_fn.word_write(single_char_count,single_char_path)
    Count_fn.word_write(bi_char_count,bi_chars_path)
    # print('文件中出现单字===>',len(single_char_count))
    # print('文件中出现双字===>',len(bi_char_count))
    num +=1


# ------------------------------------------------------------------------------------------------------------
# 每个文件中词语出现次数，每次计算10个文件，重复合并文件
batch_num = 10

# 计算每5个文件词语出现的次数
single_char_count = pd.DataFrame()
bi_char_count =pd.DataFrame()
for file_path in tqdm(save_file_list):
    # 计算每个词语出现次数～～
    single_txt =Data_Reader.reade_txt(file_path.replace('txt','single_json'),remove_space=False)
    bi_txt =Data_Reader.reade_txt(file_path.replace('txt','bi_json'),remove_space=False)
    # 出现次数合并
    single_txt =[i.split(' ') for i in single_txt]
    bi_txt =[i.split(' ') for i in bi_txt]
    single_char_df =pd.DataFrame(single_txt,columns=['word','num'])
    bi_char_df =pd.DataFrame(bi_txt,columns=['word','num'])

    single_char_df['num'] =single_char_df['num'].apply(int)
    bi_char_df['num'] =bi_char_df['num'].apply(int)


    # # 合并
    single_char_count = pd.concat([single_char_count,single_char_df])
    bi_char_count =pd.concat([bi_char_count,bi_char_df])

    #
    # # groupby之后
    single_char_count = single_char_count.groupby('word')['num'].sum().reset_index()
    bi_char_count = bi_char_count.groupby('word')['num'].sum().reset_index()
    # print(bi_char_count[0:100])
    # break


# ------------------------------------------------------------------------------------------------------------
single_char_count.to_csv('./single_gram.csv',index=False)
bi_char_count.to_csv('./bi_gram.csv',index=False)


