from matplotlib.cbook import boxplot_stats
from PAPyA.config_loader import Loader
from PAPyA.file_reader import FileReader
from PAPyA.get_ranks import getRanks
from PAPyA.nsga2 import Nsga2
import yaml
import warnings
import itertools
import plotly.express as px
import pandas as pd
from itertools import islice
import matplotlib.pyplot as plt
import scipy.stats as ss
import numpy as np
from numpy.ma.core import maximum_fill_value
import seaborn as sns
import matplotlib.pyplot as plt

# SINGEL DIMENSION


class SDRank(FileReader):
    def __init__(self, config_path: str, log_path: str, size: str, sd: str):
        super().__init__(config_path, log_path, size, sd)

    def calculateRank(self, *args):
        global rank_dataframe_rscore

        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')

        if self.sd not in d:
            raise Exception("incorrect dimension")

        # splitting dataframe according to SD
        load = FileReader(self.config_path, self.log_path, self.size, self.sd)
        delRows = []
        df = load.file_reader(args)

        for arg in args:
            if isinstance(arg, list):
                for x in arg:
                    if not isinstance(x, int):
                        raise TypeError('list must be an integer')
                    else:
                        x -= 1
                        delRows.append(x)
                # df = load.file_reader()
                df = df.drop(df.columns[delRows], axis=1)
            else:
                pass
        Dictionary = {}
        count = 0
        loop = len(d.get(self.sd))
        dfs = []
        for i in range(int(len(df.index)/len(d.get(self.sd)))):
            Dictionary['df_{}'.format(i)] = df[count:loop]
            count = loop
            loop = loop+len(d.get(self.sd))
            dfs.append(Dictionary[f'df_{i}'])

        rank_dataframe = []
        # create rank occurences
        for x in dfs:
            df_ranks = x.T
            column_names = df_ranks.columns.to_numpy().tolist()

            df_ranks_occurences = []
            for index, row in df_ranks.iterrows():
                df_ranks_occurences.append(ss.rankdata(row, method='max'))

            df_ranks_occurences = pd.DataFrame(df_ranks_occurences)

            df_transpose = df_ranks_occurences.transpose()

            rank_table = []
            for index, row in df_transpose.iterrows():
                result_row = np.zeros(len(df_transpose.index))
                for i in range(len(row)):
                    result_row[int(row[i])-1] += 1
                rank_table.append(result_row)

            rank_table = pd.DataFrame(rank_table)
            rank_table = rank_table.set_axis(column_names, axis='index')
            rank_table = rank_table.set_axis(
                ['Rank '+str(i+1) for i in range(len(column_names))], axis='columns')

        # CREATE R SCORE
            q = len(df_transpose.columns)
            d = len(rank_table.index)

            rank_score = []
            for index, row in rank_table.iterrows():
                s = 0
                for r in range(d):
                    s = s + (row[r]*(d-(r+1)) / (q*(d-1)))
                rank_score.append(s)
            rank_score = pd.DataFrame(rank_score)
            rank_score = rank_score.set_axis(column_names, axis='index')
            rank_score = rank_score.set_axis(['Result'], axis='columns')
            rank_score = pd.concat([rank_table, rank_score], axis=1)
            rank_dataframe.append(rank_score)
        rank_dataframe_rscore = pd.concat(rank_dataframe)

        if len(args) == 0:
            return rank_dataframe_rscore.sort_values(by=['Result'], ascending=False)
        elif len(args) != 0:
            options = []
            store = []
            for arg in args:
                if isinstance(arg, str):
                    options.append(arg)
                else:
                    pass
            for x in range(len(options)):
                c = c = fr'(?=.*\b{options[x]}\b)'
                store.append(c)
            command = "".join(store)
            rank_dataframe_rscore = rank_dataframe_rscore.loc[rank_dataframe_rscore.index.str.contains(
                command, regex=True)]
            return rank_dataframe_rscore.sort_values(by=['Result'], ascending=False)

    def plot(self, view: str):
        self.calculateRank()
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        options = list(d.values())
        dims = list(d.keys())

        if len(dims) > 3:
            raise Exception("can only plot 3 dimensions")

        if self.sd == dims[0]:
            if view not in options[0]:
                filter = rank_dataframe_rscore.loc[rank_dataframe_rscore.index.str.contains(
                    fr'\b{view}\b', regex=True)]

                if view in options[1]:
                    Dictionary = {}
                    loop = len(options[0])
                    count = 0
                    for i in options[2]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[0])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[2])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[0])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))

                elif view in options[2]:
                    Dictionary = {}
                    loop = len(options[0])
                    count = 0
                    for i in options[1]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[0])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[1])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[0])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))
            else:
                return "the dimension is " + dims[0]

        elif self.sd == dims[1]:
            if view not in options[1]:
                filter = rank_dataframe_rscore.loc[rank_dataframe_rscore.index.str.contains(
                    fr'\b{view}\b', regex=True)]

                if view in options[2]:
                    Dictionary = {}
                    loop = len(options[1])
                    count = 0
                    for i in options[0]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[1])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[0])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[1])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))

                elif view in options[0]:
                    Dictionary = {}
                    loop = len(options[1])
                    count = 0
                    for i in options[2]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[1])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[2])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[1])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))
            else:
                return "the dimension is " + dims[1]

        elif self.sd == dims[2]:
            if view not in options[2]:
                filter = rank_dataframe_rscore.loc[rank_dataframe_rscore.index.str.contains(
                    fr'\b{view}\b', regex=True)]

                if view in options[0]:
                    Dictionary = {}
                    loop = len(options[2])
                    count = 0
                    for i in options[1]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[2])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[1])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[2])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))

                elif view in options[1]:
                    Dictionary = {}
                    loop = len(options[2])
                    count = 0
                    for i in options[0]:
                        Dictionary['{}'.format(i)] = filter[count:loop]
                        count = loop
                        loop = loop+len(options[2])

                    key = list(Dictionary)
                    Dictionary2 = {}

                    for j in range(len(options[0])):
                        val = list(Dictionary[key[j]]['Result'])
                        Dictionary2['val{}'.format(j)] = val

                    data = np.vstack(list(Dictionary2.values()))
                    df2 = pd.DataFrame(data=data, index=list(
                        Dictionary.keys()), columns=options[2])
                    return df2.plot.bar(title=str(view), rot=0, fontsize=14, figsize=(10, 10))
            else:
                return "the dimension is " + dims[2]

    def plotRadar(self):
        warnings.simplefilter(action='ignore', category=FutureWarning)

        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        if len(dims) > 3:
            raise Exception("can only plot 3 dimensions")

        topConfig = self.calculateRank()['Result'].index.str.replace(
            '.', ' ', regex=False)
        topConfig = topConfig[0].split()

        x = topConfig[0]
        y = topConfig[1]
        z = topConfig[2]
        res = []

        for i in dims:
            self.sd = i
            self.calculateRank()
            r = rank_dataframe_rscore.loc[rank_dataframe_rscore.index.str.contains(
                fr'(?=.*\b{x}\b)(?=.*\b{y}\b)(?=.*\b{z}\b)', regex=True)]['Result']
            res.append(r[0])

        tet = []
        count = 0
        for j in dims:
            t = j + ": " + topConfig[count]
            tet.append(t)
            count += 1

        df = pd.DataFrame(dict(r=res, theta=tet))
        fig = px.line_polar(df, labels={"a": "pler"}, r='r',
                            theta='theta', line_close=True)
        return fig.show()

    def plotBox(self, q=None):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')

        if self.sd not in d:
            raise Exception("incorrect dimension")

        # splitting dataframe according to SD
        load = FileReader(self.config_path, self.log_path, self.size, self.sd)
        df = load.file_reader()
        df = df.head(10).reset_index()
        df.rename(columns={'index': 'conf'}, inplace=True)

        plt.figure(figsize=(15, 10))

        # box plot in horizontal mode
        if q != None:
            box_plot = sns.boxplot(data=df[q], palette='rainbow',
                                   orient='v',  width=.25, showfliers=True)
        elif q == None:
            box_plot = sns.boxplot(data=df, palette='rainbow',
                                   orient='v',  width=.25, showfliers=True)

        i = 0
        for xtick in box_plot.get_xticklabels():
            Q = xtick.get_text()
            min_val = boxplot_stats(df[Q])[0]['whislo']
            max_val = max(boxplot_stats(df[Q])[0]['fliers']) if (
                len(boxplot_stats(df[Q])[0]['fliers'] != 0)) else 0
            whisk_max = boxplot_stats(df[Q])[0]['whishi']

            minConf_text = df.loc[df[Q] == min_val, 'conf'].iloc[0]
            maxConf_text = df.loc[df[Q] == max_val,
                                  'conf'].iloc[0] if (max_val != 0) else ""
            whisk_max_text = df.loc[df[Q] == whisk_max, 'conf'].iloc[0]

            box_plot.text(i, min_val, minConf_text, horizontalalignment='center',
                          color='g', size='large', weight='bold')
            box_plot.text(i, max_val, maxConf_text, horizontalalignment='center',
                          color='r', size='large', weight='bold')
            box_plot.text(i, whisk_max, whisk_max_text, horizontalalignment='center',
                          color='r', size='large', weight='bold')
            i = i+1

        box_plot.set_xlabel("Query", fontsize=15)
        box_plot.set_ylabel("Runtime (in sec)", fontsize=15)
        box_plot.set(title='Query Box Plot')
        return plt.show()


# MULTI DIMENSION


class MDRank(FileReader):
    def __init__(self, config_path: str, log_path: str, size: str, sd=None):
        super().__init__(config_path, log_path, size, sd)

    def dominates(self, row, candidateRow):
        # kalo lebih kecil semua berarti dominasi
        return np.sum([row[x] <= candidateRow[x] for x in range(len(row))]) == len(row)

    def dominates_agg(self, row, candidateRow):
        return np.sum([row[x] >= candidateRow[x] for x in range(len(row))]) == len(row)

    def getConfs(self, points, orignaldf):
        point_confs = {}
        for i in range(0, len(orignaldf)):
            for val in points:
                if (list(val) == np.array(orignaldf[i][1:], dtype=np.float64).tolist()):
                    point_confs[val] = orignaldf[i][0]
        return point_confs

    def getConfsSorted(self, confsDict):
        # # summing all the values using sum()
        temp1 = {val: sum(float(idx) for idx in val)
                 for val, key in confsDict.items()}  # sum the ranks of non dominated solution

        # using sorted to perform sorting as required
        # sort according to the sum before
        temp2 = sorted(
            temp1.items(), key=lambda ele: temp1[ele[0]], reverse=False)
        res = {}
        for key, val in temp2:
            res[key] = confsDict[key]
        return res

    def paretoQ(self):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')

        self.sd = list(d.keys())[-1]
        dimensionsAll = getRanks(
            self.config_path, self.log_path, self.size, self.sd).getRanks()
        dimensionsAll = dimensionsAll.reset_index().values
        dimensions = np.array(getRanks(
            self.config_path, self.log_path, self.size, self.sd).getRanks()[:], dtype=np.float64)
        inputPoints = dimensions.tolist()
        paretoPoints, dominatedPoints = Nsga2(
            inputPoints, self.dominates).execute()

        pareto_q = self.getConfs(paretoPoints, dimensionsAll)
        pareto_q = self.getConfsSorted(pareto_q)

        vals = np.array(list(pareto_q.values()))
        table_pareto = pd.DataFrame({'Solution': vals}, columns=['Solution'])

        dominated = self.getConfs(dominatedPoints, dimensionsAll)
        dominated = self.getConfsSorted(dominated)
        vals = np.array(list(dominated.values()))
        table_dominated = pd.DataFrame(
            {'Dominated': vals}, columns=['Dominated'])

        table = pd.concat([table_pareto, table_dominated], axis=1)
        table = table.replace(np.nan, '', regex=True)
        return table

    def paretoAgg(self):
        global dominatedPoints
        global paretoPoints
        global dims

        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        li = []
        for x in dims:
            self.sd = x
            dimension_ranking = SDRank(
                self.config_path, self.log_path, self.size, self.sd).calculateRank()
            dimension_ranking = dimension_ranking['Result']
            li.append(dimension_ranking)
        new_df = pd.concat(li, axis=1)
        dimensionsAll = new_df.reset_index().values
        dimensions = np.array(dimensionsAll[:, 1:], dtype=np.float64)
        inputPoints = dimensions.tolist()
        paretoPoints, dominatedPoints = Nsga2(
            inputPoints, self.dominates_agg).execute()

        pareto_agg = self.getConfs(paretoPoints, dimensionsAll)
        pareto_agg = self.getConfsSorted(pareto_agg)
        vals = np.array(list(pareto_agg.values()))
        table_pareto = pd.DataFrame(vals, columns=['Solution'])

        dominated = self.getConfs(dominatedPoints, dimensionsAll)
        dominated = self.getConfsSorted(dominated)
        vals = np.array(list(dominated.values()))
        table_dominated = pd.DataFrame(
            {'Dominated': vals}, columns=['Dominated'])

        table = pd.concat([table_pareto, table_dominated], axis=1)
        table = table.replace(np.nan, '', regex=True)
        return table

    def plot(self):
        self.paretoAgg()

        if len(dims) > 3:
            raise Exception("can only plot 3 dimensions")

        fig = plt.figure()
        fig.set_size_inches(15, 15)
        ax = fig.add_subplot(111, projection='3d')
        dp = np.array(list(dominatedPoints))
        pp = np.array(list(paretoPoints))

        print(pp.shape, dp.shape)
        ax.scatter(dp[:, 0], dp[:, 1], dp[:, 2])
        ax.scatter(pp[:, 0], pp[:, 1], pp[:, 2], color='green')

        ax.set_xlabel('Rf')
        ax.set_ylabel('Rp')
        ax.set_zlabel('Rs')

        import matplotlib.tri as mtri
        triang = mtri.Triangulation(pp[:, 0], pp[:, 1])
        ax.plot_trisurf(triang, pp[:, 2], color='green', alpha=0.3)
        return plt.show()
