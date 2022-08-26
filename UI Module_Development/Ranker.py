from logging import exception
from tkinter.ttk import Style
from turtle import color, width
from PAPyA.config_loader import Loader
from PAPyA.get_ranks import getRanks
from PAPyA.best_of_k import bestOfParetoAgg, bestOfParetoQ, bestOfRTA, bestOfSD
from PAPyA.file_reader import FileReader
from PAPyA.kendall_index import kendallIndex
from Rank import SDRank
from Rank import MDRank
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
            elif x == 'RTA':
                self.sd = list(d.keys())[-1]
                criteria_table_RTA = getRanks(
                    self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_RTA = criteria_table_RTA.loc[bestOfRTA(
                    self.config_path, self.log_path, self.size, self.k).bestOfRTA()]
                criteria_table_RTA = criteria_table_RTA[criteria_table_RTA > self.h]
                count = criteria_table_RTA.count(axis=1)
                sum = count.sum(axis=0)
                conformance = 1 - (sum/(self.k*query))
                data.append(conformance)
                idx.append('RTA')

        table = pd.DataFrame(data, index=idx, columns=[self.size])
        return table

    def plot(self, mode=0):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        query = data.get('query')
        dims = list(d.keys())

        if mode == 0:
            data = self.run()
            value = data[self.size].to_list()
            idx = list(data.index)

            non_conformance = []
            for x in value:
                stacked = 1 - x
                non_conformance.append(stacked)

            df = pd.DataFrame(np.transpose(np.array([value, non_conformance])), index=idx, columns=[
                              "Conformance", "Non-Conformance"])
            sns.set(style='white', rc = {'figure.figsize':(10,6)})
            cmap = sns.color_palette()
            df.plot(kind='bar', stacked=True, color=[cmap[2], cmap[3]], width=0.2)
            plt.xticks(rotation='0')

            return plt.show()

        elif mode == 1:
            total_conformance = []
            config_name = []
            for x in self.li:
                if x in dims:
                    self.sd = x
                    criteria_table = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table = criteria_table.loc[bestOfSD(
                        self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                    criteria_table = criteria_table[criteria_table > self.h]
                    count = criteria_table.count(axis=1)
                    if len(count) >= 3:
                        count = count[:3]
                    elif len(count) < 3:
                        count = count[:]
                    idx = list(count.index)
                    val = count.to_list()

                    conformance_value = []
                    counter_value = []
                    for i in val:
                        conformance = 1-(i/query)
                        counter = 1 - conformance
                        conformance_value.append(conformance)
                        counter_value.append(counter)
                    total_conformance.append(conformance_value)
                    total_conformance.append(counter_value)
                    config_name.append(idx)
                elif x == 'paretoQ':
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoQ = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                    criteria_table_paretoQ = criteria_table_paretoQ[criteria_table_paretoQ > self.h]
                    count = criteria_table_paretoQ.count(axis=1)
                    if len(count) >= 3:
                        count = count[:3]
                    elif len(count) < 3:
                        count = count[:]
                    idx = list(count.index)
                    val = count.to_list()

                    conformance_value = []
                    counter_value = []
                    for i in val:
                        conformance = 1-(i/query)
                        counter = 1 - conformance
                        conformance_value.append(conformance)
                        counter_value.append(counter)
                    total_conformance.append(conformance_value)
                    total_conformance.append(counter_value)
                    config_name.append(idx)
                elif x == "paretoAgg":
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoAgg = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                    criteria_table_paretoAgg = criteria_table_paretoAgg[
                        criteria_table_paretoAgg > self.h]
                    count = criteria_table_paretoAgg.count(axis=1)
                    if len(count) >= 3:
                        count = count[:3]
                    elif len(count) < 3:
                        count = count[:]
                    idx = list(count.index)
                    val = count.to_list()

                    conformance_value = []
                    counter_value = []
                    for i in val:
                        conformance = 1-(i/query)
                        counter = 1 - conformance
                        conformance_value.append(conformance)
                        counter_value.append(counter)
                    total_conformance.append(conformance_value)
                    total_conformance.append(counter_value)
                    config_name.append(idx)
                elif x == "RTA":
                    self.sd = list(d.keys())[-1]
                    criteria_table_RTA = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_RTA = criteria_table_RTA.loc[bestOfRTA(
                        self.config_path, self.log_path, self.size, self.k).bestOfRTA()]
                    criteria_table_RTA = criteria_table_RTA[criteria_table_RTA > self.h]
                    count = criteria_table_RTA.count(axis=1)
                    if len(count) >= 3:
                        count = count[:3]
                    elif len(count) < 3:
                        count = count[:]
                    idx = list(count.index)
                    val = count.to_list()

                    conformance_value = []
                    counter_value = []
                    for i in val:
                        conformance = 1-(i/query)
                        counter = 1 - conformance
                        conformance_value.append(conformance)
                        counter_value.append(counter)
                    total_conformance.append(conformance_value)
                    total_conformance.append(counter_value)
                    config_name.append(idx)

            counter = 0
            for i in range(len(self.li)):
                bottom = total_conformance[counter]
                top = total_conformance[counter+1]
                print(bottom, top)
                counter = counter + 2

                xlabel = config_name[i]
                cmap = sns.color_palette()

                plt.title(f"{self.li[i]} ranking")
                plt.bar(xlabel, bottom, color=[cmap[2]], width=0.2)
                plt.bar(xlabel, top, bottom=bottom, color=[cmap[3]], width=0.2)
                plt.show()

    def configurationQueryRanks(self, dimension: str, mode=0):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        if mode == 0:
            if dimension in self.li:
                if dimension == "paretoQ":
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoQ = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                    return criteria_table_paretoQ
                elif dimension == "paretoAgg":
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoAgg = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                    return criteria_table_paretoAgg
                elif dimension == "RTA":
                    self.sd = list(d.keys())[-1]
                    criteria_table_RTA = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_RTA = criteria_table_RTA.loc[bestOfRTA(
                        self.config_path, self.log_path, self.size, self.k).bestOfRTA()]
                    return criteria_table_RTA
                else:
                    self.sd = dimension
                    criteria_table = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table = criteria_table.loc[bestOfSD(
                        self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                    return criteria_table

            elif dimension not in self.li:
                raise Exception("Dimension is not in set")

        elif mode == 1:
            def color_boolean(val):
                color = ''
                if val == 'True':
                    color = 'red'
                elif val == 'False':
                    color = 'green'
                return 'color: %s' % color

            if dimension in self.li:
                if dimension == "paretoQ":
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoQ = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                    criteria_table_paretoQ = criteria_table_paretoQ[criteria_table_paretoQ > self.h]
                    ordering = criteria_table_paretoQ.count(axis='columns')
                    ordering = ordering.sort_values(ascending=True)
                    ordering = ordering.index.to_list()
                    criteria_table_paretoQ = criteria_table_paretoQ.loc[ordering]
                    criteria_table_paretoQ = criteria_table_paretoQ[:].notnull(
                    )
                    criteria_table_paretoQ = criteria_table_paretoQ[:].astype(
                        str)
                    criteria_table_paretoQ = criteria_table_paretoQ.style.applymap(
                        color_boolean)
                    return criteria_table_paretoQ
                elif dimension == "paretoAgg":
                    self.sd = list(d.keys())[-1]
                    criteria_table_paretoAgg = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(
                        self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                    criteria_table_paretoAgg = criteria_table_paretoAgg[
                        criteria_table_paretoAgg > self.h]
                    ordering = criteria_table_paretoAgg.count(axis='columns')
                    ordering = ordering.sort_values(ascending=True)
                    ordering = ordering.index.to_list()
                    criteria_table_paretoAgg = criteria_table_paretoAgg.loc[ordering]
                    criteria_table_paretoAgg = criteria_table_paretoAgg[:].notnull(
                    )
                    criteria_table_paretoAgg = criteria_table_paretoAgg[:].astype(
                        str)
                    criteria_table_paretoAgg = criteria_table_paretoAgg.style.applymap(
                        color_boolean)
                    return criteria_table_paretoAgg
                elif dimension == "RTA":
                    self.sd = list(d.keys())[-1]
                    criteria_table_RTA = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table_RTA = criteria_table_RTA.loc[bestOfRTA(
                        self.config_path, self.log_path, self.size, self.k).bestOfRTA()]
                    criteria_table_RTA = criteria_table_RTA[
                        criteria_table_RTA > self.h]
                    ordering = criteria_table_RTA.count(axis='columns')
                    ordering = ordering.sort_values(ascending=True)
                    ordering = ordering.index.to_list()
                    criteria_table_RTA = criteria_table_RTA.loc[ordering]
                    criteria_table_RTA = criteria_table_RTA[:].notnull(
                    )
                    criteria_table_RTA = criteria_table_RTA[:].astype(
                        str)
                    criteria_table_RTA = criteria_table_RTA.style.applymap(
                        color_boolean)
                    return criteria_table_RTA
                else:
                    self.sd = dimension
                    criteria_table = getRanks(
                        self.config_path, self.log_path, self.size, self.sd).getRanks()
                    criteria_table = criteria_table.loc[bestOfSD(
                        self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                    criteria_table = criteria_table[criteria_table > self.h]
                    ordering = criteria_table.count(axis='columns')
                    ordering = ordering.sort_values(ascending=True)
                    ordering = ordering.index.to_list()
                    criteria_table = criteria_table.loc[ordering]
                    criteria_table = criteria_table[:].notnull()
                    criteria_table = criteria_table[:].astype(str)
                    criteria_table = criteria_table.style.applymap(
                        color_boolean)
                    return criteria_table

            elif dimension not in self.li:
                raise Exception("Dimension is not in set")


class Coherence(FileReader):
    def __init__(self, config_path: str, log_path: str, li: list, sd=None, size=None):
        super().__init__(config_path, log_path, size, sd)
        self.li = li

    def run(self, rankset1: str, rankset2: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        result = []
        for x in self.li:
            if x in dims:
                self.sd = x
                var1 = SDRank(self.config_path, self.log_path,
                              rankset1, self.sd).calculateRank()
                var1 = var1['Result'].sort_index()
                var1 = var1.to_numpy()
                var2 = SDRank(self.config_path, self.log_path,
                              rankset2, self.sd).calculateRank()
                var2 = var2['Result'].sort_index()
                var2 = var2.to_numpy()
                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path,
                              rankset1, self.sd).calculateRank()
                paretoAggSolution = MDRank(
                    self.config_path, self.log_path, rankset1, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace(
                    '', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var1 = var1.loc[paretoAggSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path,
                              rankset2, self.sd).calculateRank()
                paretoAggSolution = MDRank(
                    self.config_path, self.log_path, rankset2, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace(
                    '', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var2 = var2.loc[paretoAggSolution]
                var2 = var2['Result']

                newDf = pd.concat([var1, var2], axis=1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(rankset1)  # var1
                newDf_column_name[1] = str(rankset2)  # var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(rankset1)]
                var2 = newDf[str(rankset2)]

                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path,
                              rankset1, self.sd).calculateRank()
                paretoQSolution = MDRank(
                    self.config_path, self.log_path, rankset1, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace(
                    '', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var1 = var1.loc[paretoQSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path,
                              rankset2, self.sd).calculateRank()
                paretoQSolution = MDRank(
                    self.config_path, self.log_path, rankset2, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace(
                    '', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var2 = var2.loc[paretoQSolution]
                var2 = var2['Result']

                newDf = pd.concat([var1, var2], axis=1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(rankset1)  # var1
                newDf_column_name[1] = str(rankset2)  # var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(rankset1)]
                var2 = newDf[str(rankset2)]

                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(
                    var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == "RTA":
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path,
                              rankset1, self.sd).calculateRank()
        return pd.DataFrame(result, index=self.li, columns=['Kendall\'s Index'])

    def heatMapSubtract(self, *args, dimension: str):
        if dimension in self.li:
            if dimension == "paretoAgg":
                var = MDRank(self.config_path, self.log_path,
                             args[0]).paretoAgg()
                data1_1 = var['Solution']
                data1_1.replace('', np.nan, inplace=True)
                data1_1.dropna(inplace=True)
                data1_2 = var['Dominated']
                data1_2.replace('', np.nan, inplace=True)
                data1_2.dropna(inplace=True)
                data1 = pd.concat([data1_1, data1_2],
                                  axis=0, ignore_index=True)

                data1 = data1[:10]
                order1 = list(data1.index)
                idx = data1.tolist()
                order1 = [x+1 for x in order1]

                ylabels = []
                ordering = []
                for x in args[1:]:
                    var2 = MDRank(self.config_path,
                                  self.log_path, x).paretoAgg()
                    data2_1 = var2['Solution']
                    data2_1.replace('', np.nan, inplace=True)
                    data2_1.dropna(inplace=True)
                    data2_2 = var2['Dominated']
                    data2_2.replace('', np.nan, inplace=True)
                    data2_2.dropna(inplace=True)
                    data2 = pd.concat([data2_1, data2_2],
                                      axis=0, ignore_index=True)

                    data2 = pd.Series(data2.index.values, index=data2)
                    data2 = data2.loc[idx]
                    data2 = data2.tolist()
                    order2 = [x+1 for x in data2]

                    # CREATE YLABEL AND ORDERING
                    ylabels.append(str(args[0]) + "-" + str(x))
                    order1_2 = np.subtract(order1, order2)
                    order1_2 = np.absolute(order1_2)
                    ordering.append(order1_2)

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

            elif dimension == "paretoQ":
                var = MDRank(self.config_path, self.log_path,
                             args[0]).paretoQ()
                data1_1 = var['Solution']
                data1_1.replace('', np.nan, inplace=True)
                data1_1.dropna(inplace=True)
                data1_2 = var['Dominated']
                data1_2.replace('', np.nan, inplace=True)
                data1_2.dropna(inplace=True)
                data1 = pd.concat([data1_1, data1_2],
                                  axis=0, ignore_index=True)

                data1 = data1[:10]
                order1 = list(data1.index)
                idx = data1.tolist()
                order1 = [x+1 for x in order1]

                ylabels = []
                ordering = []
                for x in args[1:]:
                    var2 = MDRank(self.config_path, self.log_path, x).paretoQ()
                    data2_1 = var2['Solution']
                    data2_1.replace('', np.nan, inplace=True)
                    data2_1.dropna(inplace=True)
                    data2_2 = var2['Dominated']
                    data2_2.replace('', np.nan, inplace=True)
                    data2_2.dropna(inplace=True)
                    data2 = pd.concat([data2_1, data2_2],
                                      axis=0, ignore_index=True)

                    data2 = pd.Series(data2.index.values, index=data2)
                    data2 = data2.loc[idx]
                    data2 = data2.tolist()
                    order2 = [x+1 for x in data2]

                    # CREATE YLABEL AND ORDERING
                    ylabels.append(str(args[0]) + "-" + str(x))
                    order1_2 = np.subtract(order1, order2)
                    order1_2 = np.absolute(order1_2)
                    ordering.append(order1_2)

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
                    ordering.append(order1_2)

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

        elif dimension not in self.li:
            raise Exception("Dimension is not in set")

    def heatMap(self, rankset1: str, rankset2: str, dimension: str):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())

        if dimension in self.li:
            if dimension == "paretoAgg":
                data1 = MDRank(self.config_path, self.log_path,
                               rankset1).paretoAgg()
                data2 = MDRank(self.config_path, self.log_path,
                               rankset2).paretoAgg()

                data1_1 = data1['Solution']
                data1_1.replace('', np.nan, inplace=True)
                data1_1.dropna(inplace=True)
                data1_2 = data1['Dominated']
                data1_2.replace('', np.nan, inplace=True)
                data1_2.dropna(inplace=True)
                data1 = pd.concat([data1_1, data1_2],
                                  axis=0, ignore_index=True)

                data2_1 = data2['Solution']
                data2_1.replace('', np.nan, inplace=True)
                data2_1.dropna(inplace=True)
                data2_2 = data2['Dominated']
                data2_2.replace('', np.nan, inplace=True)
                data2_2.dropna(inplace=True)
                data2 = pd.concat([data2_1, data2_2],
                                  axis=0, ignore_index=True)

                data1 = data1[:10]
                order1 = list(data1.index)
                idx = data1.tolist()
                order1 = [x+1 for x in order1]

                data2 = pd.Series(data2.index.values, index=data2)
                data2 = data2.loc[idx]
                data2 = data2.tolist()
                order2 = [x+1 for x in data2]

                xlabels = idx
                ylabels = [str(rankset1), str(rankset2)]
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

            elif dimension == "paretoQ":
                data1 = MDRank(self.config_path, self.log_path,
                               rankset1).paretoQ()
                data2 = MDRank(self.config_path, self.log_path,
                               rankset2).paretoQ()

                data1_1 = data1['Solution']
                data1_1.replace('', np.nan, inplace=True)
                data1_1.dropna(inplace=True)
                data1_2 = data1['Dominated']
                data1_2.replace('', np.nan, inplace=True)
                data1_2.dropna(inplace=True)
                data1 = pd.concat([data1_1, data1_2],
                                  axis=0, ignore_index=True)

                data2_1 = data2['Solution']
                data2_1.replace('', np.nan, inplace=True)
                data2_1.dropna(inplace=True)
                data2_2 = data2['Dominated']
                data2_2.replace('', np.nan, inplace=True)
                data2_2.dropna(inplace=True)
                data2 = pd.concat([data2_1, data2_2],
                                  axis=0, ignore_index=True)

                data1 = data1[:10]
                order1 = list(data1.index)
                idx = data1.tolist()
                order1 = [x+1 for x in order1]

                data2 = pd.Series(data2.index.values, index=data2)
                data2 = data2.loc[idx]
                data2 = data2.tolist()
                order2 = [x+1 for x in data2]

                xlabels = idx
                ylabels = [str(rankset1), str(rankset2)]
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

            else:
                var1 = SDRank(self.config_path, self.log_path,
                              rankset1, dimension).calculateRank()
                var1 = var1['Result']
                var1 = var1.nlargest(n=10, keep='first')
                idx = var1.index.tolist()
                scores = var1.to_numpy()
                order1 = np.argsort(-scores, kind="stable")
                order1 = order1 + 1

                var2 = SDRank(self.config_path, self.log_path,
                              rankset2, dimension).calculateRank()
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

                xlabels = var1.index.values.tolist()
                xlabels = xlabels[:10]
                ylabels = [str(rankset1), str(rankset2)]
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

        elif dimension not in self.li:
            raise Exception("Dimension is not in set")
