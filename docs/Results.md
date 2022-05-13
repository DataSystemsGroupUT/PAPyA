# Results
This section presents PAPyA library in practice, we focus on the performance of a generalized single dimension and multi dimension ranking for complex big data solutions using a perscriptive performance analysis (PPA) approach. Our experiment design consist of:

- set of queries which are manually translated in SQL from SPARQL
- RDF datasets of different sizes 
- A configuration file based on the three dimensions (i.e. schemas, partitioning, and storage formats)

In our experiment, we evaluate the performance of SparkSQL as a relational engine for evaluating the query workload. In particular, our key performance index is the query latency, but this could be extended to other metrics of evaluation. Our analysis is based on the average result of five different runs.

### Data Preparator Part

### Bench-Ranking
Bench-Ranking phase starts when we have results from Data Preparator in log files in the log folder of our repository. To start the analysis, we need to specify all dimensions and their options along with our key performance index which in our case is the query runs.

```python
dimensions:
    schemas: ["st", "vt", "pt", "extvt", "wpt"]
    partition: ["horizontal", "predicate", "subject"]
    storage: ["csv", "avro", "parquet", "orc"]
query: 11
```

In this experiment, we call calculateRankScore method to the single dimension rank for the "schema" dimension along with its dataset size for calculating the rank scores of this dimension.

```python
def calculateRankScore(self, sd:str, size:str):
        data = self.loader()
        d = data.get('dimensions')
        
        if sd not in d:
            raise Exception("incorrect dimension")
        
        
        #splitting dataframe according to SD
        df = self.file_reader(sd, size)
        Dictionary = {}
        count = 0
        loop = len(d.get(sd))
        dfs = []
        
        for i in range(int(len(self.joined_string)/len(d.get(sd)))):
            Dictionary['df_{}'.format(i)] = df[count:loop]
            count = loop
            loop = loop+len(d.get(sd))
            dfs.append(Dictionary[f'df_{i}'])
            
        rank_dataframe = []
        # create rank occurences
        for x in dfs:
            df_ranks = x.T
            column_names = df_ranks.columns.to_numpy().tolist()
            
            df_ranks_occurences = []
            for index, row in df_ranks.iterrows():
                df_ranks_occurences.append(ss.rankdata(row, method = 'max'))

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
            rank_table = rank_table.set_axis(['Rank '+str(i+1) for i in range(len(column_names))], axis='columns')
            
            # CREATE R SCORE
            q = data.get('query')
            d = len(rank_table.index)

            rank_score = []
            for index, row in rank_table.iterrows():
                s=0
                for r in range(d):  
                    s = s + (row[r]*(d-(r+1)) / (q*(d-1)) )
                rank_score.append(s)
            rank_score = pd.DataFrame(rank_score)
            rank_score = rank_score.set_axis(column_names, axis = 'index')
            rank_score = rank_score.set_axis(['Result'], axis='columns')
            rank_score = pd.concat([rank_table, rank_score], axis = 1)
            rank_dataframe.append(rank_score)
        self.rank_dataframe_rscore = pd.concat(rank_dataframe)
        return self.rank_dataframe_rscore.sort_values(by = ['Result'], ascending = False)

schemaSDRank = singleDimensionRanking.calculateRankScore('schemas', '100M')
```
