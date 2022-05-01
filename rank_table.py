import pandas as pd
import numpy as np

xls = pd.ExcelFile("~/Desktop/query_ranks.xlsx")
df = pd.read_excel(xls, 'query-ranks-100M', index_col = 0)
df_ranks=df.rank(axis = 0, na_option = 'keep', numeric_only=True, method='max')
df = pd.concat([df, df_ranks], axis=1)
print(df)

xlwriter = pd.ExcelWriter('~/Desktop/query_ranks.xlsx')
df.to_excel(xlwriter, 'query-ranks-100M')
xlwriter.close()


# xls = pd.ExcelFile("~/Desktop/results.xlsx")
# df_avro = pd.read_excel(xls, 'Avro')
# df_csv = pd.read_excel(xls, 'CSV')

# df_avro = df_avro.drop(index = 0 , columns = 'Avro')
# df_csv = df_csv.drop(index = 0, columns = 'CSV')

# df_csv_ranks = []
# df_avro_ranks = []
# for index, row in df_csv.iterrows():
#     # print(index, row[0])
#     df_csv_ranks.append(ss.rankdata(row))

# for index,row in df_avro.iterrows():
#     df_avro_ranks.append(ss.rankdata(row))

# df_csv_ranks = pd.DataFrame(df_csv_ranks)
# df_avro_ranks = pd.DataFrame(df_avro_ranks)
# df_csv_transpose = df_csv_ranks.transpose()
# df_avro_transpose = df_avro_ranks.transpose()

# #create table
# csv_rank_table = []
# for index, row in df_csv_transpose.iterrows():
#     result_row = np.zeros(5)  
#     for i in range(len(row)):
#         result_row[int(row[i])-1] +=1
#     csv_rank_table.append(result_row)
# csv_rank_table = pd.DataFrame(csv_rank_table)

# avro_rank_table = []
# for index, row in df_avro_transpose.iterrows():
#     result_row = np.zeros(5)  
#     for i in range(len(row)):
#         result_row[int(row[i])-1] +=1
#     avro_rank_table.append(result_row)
# avro_rank_table = pd.DataFrame(avro_rank_table)

# #export to excel
# csv_rank_table = csv_rank_table.set_axis(['ST', 'VT', 'PT', 'ExtVP', 'WPT'], axis = 'index')
# csv_rank_table = csv_rank_table.set_axis(['1st', '2nd', '3rd', '4th', '5th'], axis='columns')

# avro_rank_table = avro_rank_table.set_axis(['ST', 'VT', 'PT', 'ExtVP', 'WPT'], axis = 'index')
# avro_rank_table = avro_rank_table.set_axis(['1st', '2nd', '3rd', '4th', '5th'], axis='columns')

# xlwriter = pd.ExcelWriter('~/Desktop/rankings.xlsx')
# csv_rank_table.to_excel(xlwriter, 'CSV-HP-100M')
# avro_rank_table.to_excel(xlwriter, 'Avro-HP-100M')
# xlwriter.close()