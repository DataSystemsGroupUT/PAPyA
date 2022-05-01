import pandas as pd
import re
from sympy import false
import glob
from collections import OrderedDict

# s = './logs/csv/100M_st_predicate.txt'
# s = s.rsplit('/', 1)[1]
# print(s)
def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

path = './logs/csv/'
path2 = './logs/avro/'
path3 = './logs/orc/'
path4 = './logs/parquet/'
all_files_csv = sorted(glob.glob(path+"100M_*.txt"))
all_files_avro = sorted(glob.glob(path2+"100M_*.txt"))
all_files_orc = sorted(glob.glob(path3+"100M_*.txt"))
all_files_parquet = sorted(glob.glob(path4+"100M_*.txt"))

# CSV LOOP
li = []
for i in all_files_csv:
    df = pd.read_csv(i, sep = ',', header= None)
    df = df.fillna(0)
    avg = df.mean(axis = 0)
    idx = i.rsplit('/', 1)[1]
    for r in (('100M_',''),('extvt', 'd'), ('st', 'a'), ('vt', 'b'), ('wpt', 'e'), ('pt', 'c'),('horizontal', 'i'), ('subject', 'ii'), ('predicate', 'iii'),('_','.'),('txt', '')):
        idx  = idx.replace(*r)
    df = pd.DataFrame(avg.values, columns=[idx + '2'])
    df = df.transpose()
    li.append(df)

# AVRO LOOP
for i in all_files_avro:
    df = pd.read_csv(i, sep = ',', header= None)
    df = df.fillna(0)
    avg = df.mean(axis = 0)
    idx = i.rsplit('/', 1)[1]
    for r in (('100M_', ''), ('extvt', 'd'), ('st', 'a'), ('vt', 'b'), ('wpt', 'e'), ('pt', 'c'),('horizontal', 'i'), ('subject', 'ii'), ('predicate', 'iii'),('_','.'),('txt', '')):
        idx = idx.replace(*r)
    df = pd.DataFrame(avg.values, columns=[idx + '1'])
    df = df.transpose()
    li.append(df)

# # ORC LOOP
for i in all_files_orc:
    df = pd.read_csv(i, sep = ',', header= None)
    df = df.fillna(0)
    avg = df.mean(axis = 0)
    idx = i.rsplit('/', 1)[1]
    for r in (('100M_', ''), ('extvt', 'd'), ('st', 'a'), ('vt', 'b'), ('wpt', 'e'), ('pt', 'c'),('horizontal', 'i'), ('subject', 'ii'), ('predicate', 'iii'),('_','.'),('txt', '')):
        idx = idx.replace(*r)
    df = pd.DataFrame(avg.values, columns=[idx + '3'])
    df = df.transpose()
    li.append(df)

# # PARQUET LOOP
for i in all_files_parquet:
    df = pd.read_csv(i, sep = ',', header= None)
    df = df.fillna(0)
    avg = df.mean(axis = 0)
    idx = i.rsplit('/', 1)[1]
    for r in (('100M_', ''), ('extvt', 'd'), ('st', 'a'), ('vt', 'b'), ('wpt', 'e'), ('pt', 'c'),('horizontal', 'i'), ('subject', 'ii'), ('predicate', 'iii'),('_','.'),('txt', '')):
        idx = idx.replace(*r)
    df = pd.DataFrame(avg.values, columns=[idx + '4'])
    df = df.transpose()
    li.append(df)

df = pd.concat(li, axis = 0)
queries = ["Q"+str(i+1) for i in range(len(df.columns))]
df = df.set_axis(queries, axis = 'columns')
df = df.fillna(5000)
print(df)

xlwriter = pd.ExcelWriter('~/Desktop/query_ranks.xlsx')
df.to_excel(xlwriter, 'query-ranks-100M')
xlwriter.close()


# for i in data:
#     df1 = pd.read_csv(data, sep = ',', header = None)
#     df1 = df1.fillna(0)
#     avg = df1.mean(axis = 0)
#     queries = ["Q"+str(i+1) for i in range(11)]
#     df1 = pd.DataFrame(avg.values, columns=['avg_result'], index=[queries])
#     df1 = df1.transpose()
#     print(df1)



# df2 = pd.read_csv('./logs/csv/st/100M_predicate.txt', sep = ',', header = None)
# df2 = df2.fillna(0)
# avg = df2.mean(axis = 0)
# queries = ["Q"+str(i+1) for i in range(11)]
# df2 = pd.DataFrame(avg.values, columns=['avg_result'], index=[queries])
# df2 = df2.transpose()
# print(df2)

# df3 = pd.read_csv('./logs/csv/st/100M_predicate.txt', sep = ',', header = None)
# df3 = df3.fillna(0)
# avg = df3.mean(axis = 0)
# queries = ["Q"+str(i+1) for i in range(11)]
# df3 = pd.DataFrame(avg.values, columns=['avg_result'], index=[queries])
# df3 = df3.transpose()
# print(df3)