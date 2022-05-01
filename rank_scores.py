import pandas as pd
import numpy as np
import scipy.stats as ss

#RANK OCCURENCES PART
xls = pd.ExcelFile("~/Desktop/query_ranks.xlsx")
df = pd.read_excel(xls, 'query-ranks-100M', index_col = 0)

#transpose first for ranking data
new_df = df.transpose()
column_names = new_df.columns.to_numpy().tolist()

#start calculating occurences
df_ranks_occurences = []
for index, row in new_df.iterrows():
    df_ranks_occurences.append(ss.rankdata(row))

df_ranks_occurences = pd.DataFrame(df_ranks_occurences)

df_transpose = df_ranks_occurences.transpose()

rank_table = []
for index, row in df_transpose.iterrows():
    result_row = np.zeros(len(df_transpose.index))  
    for i in range(len(row)):
        result_row[int(row[i])-1] +=1
    rank_table.append(result_row)
    
rank_table = pd.DataFrame(rank_table)

rank_table = rank_table.set_axis(column_names, axis = 'index')
rank_table = rank_table.set_axis([i+1 for i in range(len(column_names))], axis='columns')
print(rank_table)

xlwriter = pd.ExcelWriter('~/Desktop/query_ranks.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists = 'replace')
rank_table.to_excel(xlwriter, 'query-ranks-score-100M')
xlwriter.close()


#RANK SCORE PART
xls = pd.ExcelFile("~/Desktop/query_ranks.xlsx")
df = pd.read_excel(xls, 'query-ranks-score-100M', index_col = 0)

q=11    #total number of queries
d=36     #number of file formats

rank_score = []

#r is a rank number 1<=r<=d
for index, row in df.iterrows():
    s = 0
    for r in range(d):  
        s = s + (row[r+1]*(d-(r+1)) / (q*(d-1)) )
    # print(index, "\t", s)
    rank_score.append(s)

rank_score = pd.DataFrame(rank_score)
rank_score = rank_score.set_axis(column_names, axis = 'index')
rank_score = rank_score.set_axis(['Result'], axis='columns')
print(rank_score)

xlwriter = pd.ExcelWriter('~/Desktop/query_ranks.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists = 'replace')
rank_score.to_excel(xlwriter, 'query-ranks-score-100M')
xlwriter.close()

#CREATE RANK TABLE
xls = pd.ExcelFile("~/Desktop/query_ranks.xlsx")
df = pd.read_excel(xls, 'query-ranks-100M', index_col = 0)
df_ranks=df.rank(axis = 0, na_option = 'keep', numeric_only=True, method='first')
df = pd.concat([df, df_ranks], axis=1)

xlwriter = pd.ExcelWriter('~/Desktop/query_ranks.xlsx', mode = 'a', engine='openpyxl', if_sheet_exists = 'replace')
df.to_excel(xlwriter, 'query-ranks-100M')
xlwriter.close()