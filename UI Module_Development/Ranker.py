from logging import exception
from PAPyA.config_loader import Loader
from PAPyA.get_ranks import getRanks
from PAPyA.best_of_k import bestOfParetoAgg, bestOfParetoQ, bestOfSD
from PAPyA.file_reader import FileReader
from PAPyA.kendall_index import kendallIndex
from PAPyA.Rank import SDRank
from PAPyA.Rank import MDRank
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np

# RANKING VALIDATION


class Conformance(FileReader):
    def __init__(self, config_path: str, log_path: str, size: str, li: list, k: int, h: int, sd=None):
        super().__init__(config_path, log_path, size, sd)
        self.k = k
        self.h = h
        self.li = li

    def run(self):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        query = data.get('query')
        dims = list(d.keys())

        data = []
        idx = []

        for x in self.li:
            if x in dims:
                self.sd = x
                criteria_table = getRanks(
                    self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table = criteria_table.loc[bestOfSD(
                    self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                criteria_table = criteria_table[criteria_table > self.h]
                count = criteria_table.count(axis=1)
                sum = count.sum(axis=0)
                conformance = 1-(sum/(self.k*query))
                data.append(conformance)
                idx.append(x)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoQ = getRanks(
                    self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                    self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                criteria_table_paretoQ = criteria_table_paretoQ[criteria_table_paretoQ > self.h]
                count = criteria_table_paretoQ.count(axis=1)
                sum = count.sum(axis=0)
                conformance = 1 - (sum/(self.k*query))
                data.append(conformance)
                idx.append('paretoQ')
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoAgg = getRanks(
                    self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                    self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                criteria_table_paretoAgg = criteria_table_paretoAgg[criteria_table_paretoAgg > self.h]
                count = criteria_table_paretoAgg.count(axis=1)
                sum = count.sum(axis=0)
                conformance = 1 - (sum/(self.k*query))
                data.append(conformance)
                idx.append('paretoAgg')

        table = pd.DataFrame(data, index=idx, columns=[self.size])
        return table

    def showCriteriaTable(self, dimension: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        # for x in self.li:
        if dimension in dims:
            self.sd = dimension
            criteria_table = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table = criteria_table.loc[bestOfSD(
                self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
            return criteria_table
        elif dimension == 'paretoQ':
            self.sd = list(d.keys())[-1]
            criteria_table_paretoQ = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
            return criteria_table_paretoQ
        elif dimension == 'paretoAgg':
            self.sd = list(d.keys())[-1]
            criteria_table_paretoAgg = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
            return criteria_table_paretoAgg

    def showTable(self, dimension: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        def color_boolean(val):
            color = ''
            if val == 'True':
                color = 'green'
            elif val == 'False':
                color = 'red'
            return 'color: %s' % color

        # for x in self.li:
        if dimension in dims:
            self.sd = dimension
            criteria_table = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table = criteria_table.loc[bestOfSD(
                self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
            criteria_table = criteria_table[criteria_table > self.h]
            ordering = criteria_table.count(axis='columns')
            ordering = ordering.sort_values(ascending=False)
            ordering = ordering.index.to_list()
            criteria_table = criteria_table.loc[ordering]
            criteria_table = criteria_table[:].notnull()
            criteria_table = criteria_table[:].astype(str)
            criteria_table = criteria_table.style.applymap(color_boolean)
            return criteria_table
        elif dimension == 'paretoQ':
            self.sd = list(d.keys())[-1]
            criteria_table_paretoQ = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
            criteria_table_paretoQ = criteria_table_paretoQ[criteria_table_paretoQ > self.h]
            ordering = criteria_table_paretoQ.count(axis='columns')
            ordering = ordering.sort_values(ascending=False)
            ordering = ordering.index.to_list()
            criteria_table_paretoQ = criteria_table_paretoQ.loc[ordering]
            criteria_table_paretoQ = criteria_table_paretoQ[:].notnull()
            criteria_table_paretoQ = criteria_table_paretoQ[:].astype(str)
            criteria_table_paretoQ = criteria_table_paretoQ.style.applymap(
                color_boolean)
            return criteria_table_paretoQ
        elif dimension == 'paretoAgg':
            self.sd = list(d.keys())[-1]
            criteria_table_paretoAgg = getRanks(
                self.config_path, self.log_path, self.size, self.sd).getRanks()
            criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
            criteria_table_paretoAgg = criteria_table_paretoAgg[criteria_table_paretoAgg > self.h]
            ordering = criteria_table_paretoAgg.count(axis='columns')
            ordering = ordering.sort_values(ascending=False)
            ordering = ordering.index.to_list()
            criteria_table_paretoAgg = criteria_table_paretoAgg.loc[ordering]
            criteria_table_paretoAgg = criteria_table_paretoAgg[:].notnull()
            criteria_table_paretoAgg = criteria_table_paretoAgg[:].astype(str)
            criteria_table_paretoAgg = criteria_table_paretoAgg.style.applymap(
                color_boolean)
            return criteria_table_paretoAgg


class Coherence(FileReader):
    def __init__(self, config_path: str, log_path: str, li: list, rankset1: str, rankset2: str, sd=None, size=None):
        super().__init__(config_path, log_path, size, sd)
        self.li = li
        self.rankset1 = rankset1
        self.rankset2 = rankset2

    def run(self):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        result = []
        for x in self.li:
            if x in dims:
                self.sd = x
                var1 = SDRank(self.config_path, self.log_path,
                              self.rankset1, self.sd).calculateRank()
                var1 = var1['Result'].sort_index()
                var1 = var1.to_numpy()
                var2 = SDRank(self.config_path, self.log_path,
                              self.rankset2, self.sd).calculateRank()
                var2 = var2['Result'].sort_index()
                var2 = var2.to_numpy()

                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path,
                              self.rankset1, self.sd).calculateRank()
                paretoAggSolution = MDRank(
                    self.config_path, self.log_path, self.rankset1, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace(
                    '', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var1 = var1.loc[paretoAggSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path,
                              self.rankset2, self.sd).calculateRank()
                paretoAggSolution = MDRank(
                    self.config_path, self.log_path, self.rankset2, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace(
                    '', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var2 = var2.loc[paretoAggSolution]
                var2 = var2['Result']

                newDf = pd.concat([var1, var2], axis=1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1)  # var1
                newDf_column_name[1] = str(self.rankset2)  # var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]

                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path,
                              self.rankset1, self.sd).calculateRank()
                paretoQSolution = MDRank(
                    self.config_path, self.log_path, self.rankset1, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace(
                    '', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var1 = var1.loc[paretoQSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path,
                              self.rankset2, self.sd).calculateRank()
                paretoQSolution = MDRank(
                    self.config_path, self.log_path, self.rankset2, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace(
                    '', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var2 = var2.loc[paretoQSolution]
                var2 = var2['Result']

                newDf = pd.concat([var1, var2], axis=1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1)  # var1
                newDf_column_name[1] = str(self.rankset2)  # var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]

                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
        return pd.DataFrame(result, index=self.li, columns=['Kendall\'s Index'])

    def showTable2(self, *args, dimension: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        # for x in self.li:
        if dimension in self.li:
            var = SDRank(self.config_path, self.log_path,
                         args[0], dimension).calculateRank()
            var = var['Result']
            var = var.nlargest(n=10, keep='first')
            idx = var.index.tolist()
            scores = var.to_numpy()
            order = np.argsort(-scores)
            order = order + 1

            ylabels = []
            ordering = []
            for x in args[1:]:
                var2 = SDRank(self.config_path, self.log_path,
                              x, dimension).calculateRank()
                scores = var2['Result'].to_numpy()
                val = np.argsort(-scores, kind='stable')
                # Drop that column
                var2.drop(['Result'], axis=1, inplace=True)
                # Put whatever series you want in its place
                var2['Result'] = val
                var2 = var2['Result']

                var2 = var2.loc[idx]
                order2_list = var2[:].tolist()
                order2 = [x+1 for x in order2_list]
                order2 = np.asarray(order2)

                # CREATE YLABEL AND ORDERING
                ylabels.append(str(args[0]) + "-" + str(x))
                order1_2 = np.subtract(order, order2)
                order1_2 = np.absolute(order1_2)
                order_subtract = np.sort(order1_2)
                ordering.append(order_subtract)

            orders = np.stack((ordering), axis=0)
            xlabels = idx

            plt.rc('xtick', labelsize=8)
            plt.rc('ytick', labelsize=14)
            plt.figure(figsize=(22, 5))
            sns.heatmap(orders,
                        cmap='YlOrBr',
                        vmin=0,
                        xticklabels=xlabels,
                        yticklabels=ylabels,
                        annot=True,
                        square=True,
                        annot_kws={'fontsize': 8, 'fontweight': 'bold'})
            plt.yticks(rotation=0)
            plt.tick_params(
                which='both',
                bottom=False,
                left=False,
                labelbottom=False,
                labeltop=True)
            return plt.tight_layout()
        else:
            raise Exception("Dimension is not in set")
        #     # DATASET 1
        #     self.sd = dimension
        #     var1 = SDRank(self.config_path, self.log_path,
        #                   self.rankset1, self.sd).calculateRank()
        #     var1 = var1['Result']
        #     var1 = var1.nlargest(n=10, keep='first')
        #     idx = var1.index.tolist()
        #     scores = var1.to_numpy()
        #     order1 = np.argsort(-scores)
        #     order1 = order1 + 1
        #     # DATASET 2
        #     var2 = SDRank(self.config_path, self.log_path,
        #                   self.rankset2, self.sd).calculateRank()
        #     scores = var2['Result'].to_numpy()
        #     val = np.argsort(-scores)
        #     # Drop that column
        #     var2.drop(['Result'], axis=1, inplace=True)
        #     # Put whatever series you want in its place
        #     var2['Result'] = val
        #     var2 = var2['Result']

        #     var2 = var2.loc[idx]
        #     order2_list = var2[:].tolist()
        #     order2 = [x+1 for x in order2_list]
        #     order2 = np.asarray(order2)
        #     # DATASET 3
        #     var3 = SDRank(self.config_path, self.log_path,
        #                   '500M', self.sd).calculateRank()
        #     scores = var3['Result'].to_numpy()
        #     val = np.argsort(-scores)
        #     # Drop that column
        #     var3.drop(['Result'], axis=1, inplace=True)
        #     # Put whatever series you want in its place
        #     var3['Result'] = val
        #     var3 = var3['Result']

        #     var3 = var3.loc[idx]
        #     order3_list = var3[:].tolist()
        #     order3 = [x+1 for x in order3_list]
        #     order3 = np.asarray(order3)

        #     # ORDERING PART
        #     ylabels = [str(self.rankset1 + "-" + str(self.rankset2)),
        #                str(self.rankset1 + "-" + "500M"), str(self.rankset2 + "-" + "500M")]
        #     order1_2 = np.subtract(order1, order2)
        #     order1_2 = np.absolute(order1_2)
        #     order1 = np.sort(order1_2)

        #     order1_3 = np.subtract(order1, order3)
        #     order1_3 = np.absolute(order1_3)
        #     order2 = np.sort(order1_3)

        #     order2_3 = np.subtract(order2, order3)
        #     order2_3 = np.absolute(order2_3)
        #     order3 = np.sort(order2_3)

        #     orders = np.stack((order1, order2, order3), axis=0)

        #     # new_df_subtract = pd.DataFrame(
        #     #     orders, index=var1.index.values.tolist(), columns=['Order'])
        #     # new_df_subtract = new_df_subtract.sort_values('Order')

        #     xlabels = idx
        #     # data = new_df_subtract.values.flatten()

        #     plt.rc('xtick', labelsize=8)
        #     plt.rc('ytick', labelsize=14)
        #     plt.figure(figsize=(22, 5))
        #     sns.heatmap(orders,
        #                 cmap='YlOrBr',
        #                 vmin=0,
        #                 xticklabels=xlabels,
        #                 yticklabels=ylabels,
        #                 annot=True,
        #                 square=True,
        #                 annot_kws={'fontsize': 8, 'fontweight': 'bold'})
        #     plt.yticks(rotation=0)
        #     plt.tick_params(
        #         which='both',
        #         bottom=False,
        #         left=False,
        #         labelbottom=False,
        #         labeltop=True)
        #     return plt.tight_layout()

    def showTable(self, dimension: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        # for x in self.li:
        if dimension in dims:
            self.sd = dimension
            var1 = SDRank(self.config_path, self.log_path,
                          self.rankset1, self.sd).calculateRank()
            var1 = var1['Result']
            var1 = var1.nlargest(n=10, keep='first')
            idx = var1.index.tolist()
            scores = var1.to_numpy()
            order1 = np.argsort(-scores)
            order1 = order1 + 1

            var2 = SDRank(self.config_path, self.log_path,
                          self.rankset2, self.sd).calculateRank()
            scores = var2['Result'].to_numpy()
            val = np.argsort(-scores)
            # Drop that column
            var2.drop(['Result'], axis=1, inplace=True)
            # Put whatever series you want in its place
            var2['Result'] = val
            var2 = var2['Result']

            var2 = var2.loc[idx]
            order2_list = var2[:].tolist()
            order2 = [x+1 for x in order2_list]
            order2 = np.asarray(order2)

            xlabels = var1.index.values.tolist()
            xlabels = xlabels[:10]
            ylabels = [str(self.rankset1), str(self.rankset2)]
            orders = np.stack((order1, order2), axis=0)

            plt.rc('xtick', labelsize=10)
            plt.rc('ytick', labelsize=14)
            plt.figure(figsize=(22, 5))
            sns.heatmap(orders,
                        cmap='YlOrBr',
                        vmin=0,
                        xticklabels=xlabels,
                        yticklabels=ylabels,
                        annot=True,
                        square=True,
                        annot_kws={'fontsize': 8, 'fontweight': 'bold'})
            plt.yticks(rotation=0)
            plt.tick_params(
                which='both',
                bottom=False,
                left=False,
                labelbottom=False,
                labeltop=True)
            return plt.tight_layout()

        elif dimension not in dims:
            raise Exception("Dimension is not in config file")

        # elif x == 'paretoAgg':
        #     self.sd = list(d.keys())[-1]
        #     var1 = SDRank(self.config_path, self.log_path,
        #                     self.rankset1, self.sd).calculateRank()
        #     paretoAggSolution = MDRank(
        #         self.config_path, self.log_path, self.rankset1, self.sd).paretoAgg()
        #     paretoAggSolution = paretoAggSolution['Solution'].replace(
        #         '', np.nan)
        #     paretoAggSolution = paretoAggSolution.dropna()
        #     var1 = var1.loc[paretoAggSolution]
        #     var1 = var1['Result']
        #     var2 = SDRank(self.config_path, self.log_path,
        #                     self.rankset2, self.sd).calculateRank()
        #     paretoAggSolution = MDRank(
        #         self.config_path, self.log_path, self.rankset2, self.sd).paretoAgg()
        #     paretoAggSolution = paretoAggSolution['Solution'].replace(
        #         '', np.nan)
        #     paretoAggSolution = paretoAggSolution.dropna()
        #     var2 = var2.loc[paretoAggSolution]
        #     var2 = var2['Result']

        #     print(var1)
        #     print(var2)
        #     # newDf = pd.concat([var1, var2], axis=1)
        #     # newDf_column_name = newDf.columns.values
        #     # newDf_column_name[0] = str(self.rankset1)  # var1
        #     # newDf_column_name[1] = str(self.rankset2)  # var2

        #     # newDf.columns = newDf_column_name
        #     # newDf = newDf.fillna(1)
        #     # var1 = newDf[str(self.rankset1)]
        #     # var2 = newDf[str(self.rankset2)]

        #     # table = pd.concat([var1, var2], axis=1)
        #     # print(table)
        # elif x == 'paretoQ':
        #     self.sd = list(d.keys())[-1]
        #     var1 = SDRank(self.config_path, self.log_path,
        #                     self.rankset1, self.sd).calculateRank()
        #     paretoQSolution = MDRank(
        #         self.config_path, self.log_path, self.rankset1, self.sd).paretoQ()
        #     paretoQSolution = paretoQSolution['Solution'].replace(
        #         '', np.nan)
        #     paretoQSolution = paretoQSolution.dropna()
        #     var1 = var1.loc[paretoQSolution]
        #     var1 = var1['Result']
        #     var2 = SDRank(self.config_path, self.log_path,
        #                     self.rankset2, self.sd).calculateRank()
        #     paretoQSolution = MDRank(
        #         self.config_path, self.log_path, self.rankset2, self.sd).paretoQ()
        #     paretoQSolution = paretoQSolution['Solution'].replace(
        #         '', np.nan)
        #     paretoQSolution = paretoQSolution.dropna()
        #     var2 = var2.loc[paretoQSolution]
        #     var2 = var2['Result']

        #     newDf = pd.concat([var1, var2], axis=1)
        #     newDf_column_name = newDf.columns.values
        #     newDf_column_name[0] = str(self.rankset1)  # var1
        #     newDf_column_name[1] = str(self.rankset2)  # var2

        #     newDf.columns = newDf_column_name
        #     newDf = newDf.fillna(1)
        #     var1 = newDf[str(self.rankset1)]
        #     var2 = newDf[str(self.rankset2)]

        #     table = pd.concat([var1, var2], axis=1)
        #     print(table)
