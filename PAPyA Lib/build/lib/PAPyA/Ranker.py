from PAPyA.config_loader import Loader
from PAPyA.get_ranks import getRanks
from PAPyA.best_of_k import bestOfParetoAgg, bestOfParetoQ, bestOfSD
from PAPyA.file_reader import FileReader
from PAPyA.kendall_index import kendallIndex
from PAPyA.Rank import SDRank
from PAPyA.Rank import MDRank
import pandas as pd
import numpy as np

# RANKING VALIDATION
class Conformance(FileReader):
    def __init__(self, config_path: str, log_path: str, size: str, li:list, k:int, h:int, sd = None):
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
                criteria_table = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table = criteria_table.loc[bestOfSD(self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                criteria_table = criteria_table[criteria_table>self.h]
                count = criteria_table.count(axis = 1)
                sum = count.sum(axis = 0)
                conformance = 1-(sum/(self.k*query))
                data.append(conformance)
                idx.append(x)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoQ = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                criteria_table_paretoQ = criteria_table_paretoQ[criteria_table_paretoQ>self.h]
                count = criteria_table_paretoQ.count(axis = 1)
                sum = count.sum(axis=0)
                conformance = 1 - (sum/(self.k*query))
                data.append(conformance)
                idx.append('paretoQ')
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoAgg = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                criteria_table_paretoAgg = criteria_table_paretoAgg[criteria_table_paretoAgg>self.h]
                count = criteria_table_paretoAgg.count(axis = 1)
                sum = count.sum(axis=0)
                conformance = 1 - (sum/(self.k*query))
                data.append(conformance)
                idx.append('paretoAgg')
        
        table = pd.DataFrame(data,index = idx, columns=[self.size])
        return table
    
    def showTable(self):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())
        
        for x in self.li:
            if x in dims:
                self.sd = x
                criteria_table = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table = criteria_table.loc[bestOfSD(self.config_path, self.log_path, self.size, self.sd, self.k).bestOfSD()]
                print(criteria_table)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoQ = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoQ = criteria_table_paretoQ.loc[bestOfParetoQ(self.config_path, self.log_path, self.size, self.k).bestOfParetoQ()]
                print(criteria_table_paretoQ)
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                criteria_table_paretoAgg = getRanks(self.config_path, self.log_path, self.size, self.sd).getRanks()
                criteria_table_paretoAgg = criteria_table_paretoAgg.loc[bestOfParetoAgg(self.config_path, self.log_path, self.size, self.k).bestOfParetoAgg()]
                print(criteria_table_paretoAgg)
        
class Coherence(FileReader):
    def __init__(self, config_path: str, log_path: str, li:list, rankset1:str, rankset2:str ,sd = None, size = None):
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
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                var1 = var1['Result'].sort_index()
                var1 = var1.to_numpy()
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                var2 = var2['Result'].sort_index()
                var2 = var2.to_numpy()
                
                kendall = kendallIndex(var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                paretoAggSolution = MDRank(self.config_path, self.log_path, self.rankset1, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace('', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var1 = var1.loc[paretoAggSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                paretoAggSolution = MDRank(self.config_path, self.log_path, self.rankset2, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace('', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var2 = var2.loc[paretoAggSolution]
                var2 = var2['Result']
                
                newDf = pd.concat([var1, var2], axis = 1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1) #var1
                newDf_column_name[1] = str(self.rankset2) #var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]
                
                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                paretoQSolution = MDRank(self.config_path, self.log_path, self.rankset1, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace('', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var1 = var1.loc[paretoQSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                paretoQSolution = MDRank(self.config_path, self.log_path, self.rankset2, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace('', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var2 = var2.loc[paretoQSolution]
                var2 = var2['Result']
                
                newDf = pd.concat([var1, var2], axis = 1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1) #var1
                newDf_column_name[1] = str(self.rankset2) #var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]
                
                var1 = var1.to_numpy()
                var2 = var2.to_numpy()
                kendall = kendallIndex(var1, var2).normalised_kendall_tau_distance()
                result.append(kendall)
        return pd.DataFrame(result, index = self.li, columns = ['Kendall\'s Index'])
    
    def showTable(self):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        dims = list(d.keys())
        
        for x in self.li: 
            if x in dims:
                self.sd = x
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                var1 = var1['Result'].sort_index()
                
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                var2 = var2['Result'].sort_index()
                
                newDf = pd.concat([var1, var2], axis = 1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1) #var1
                newDf_column_name[1] = str(self.rankset2) #var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]
                
                table = pd.concat([var1, var2], axis = 1)
                print(table)
            elif x == 'paretoAgg':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                paretoAggSolution = MDRank(self.config_path, self.log_path, self.rankset1, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace('', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var1 = var1.loc[paretoAggSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                paretoAggSolution = MDRank(self.config_path, self.log_path, self.rankset2, self.sd).paretoAgg()
                paretoAggSolution = paretoAggSolution['Solution'].replace('', np.nan)
                paretoAggSolution = paretoAggSolution.dropna()
                var2 = var2.loc[paretoAggSolution]
                var2 = var2['Result']
                
                newDf = pd.concat([var1, var2], axis = 1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1) #var1
                newDf_column_name[1] = str(self.rankset2) #var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]
                
                table = pd.concat([var1, var2], axis = 1)
                print(table)
            elif x == 'paretoQ':
                self.sd = list(d.keys())[-1]
                var1 = SDRank(self.config_path, self.log_path, self.rankset1, self.sd).calculateRank()
                paretoQSolution = MDRank(self.config_path, self.log_path, self.rankset1, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace('', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var1 = var1.loc[paretoQSolution]
                var1 = var1['Result']
                var2 = SDRank(self.config_path, self.log_path, self.rankset2, self.sd).calculateRank()
                paretoQSolution = MDRank(self.config_path, self.log_path, self.rankset2, self.sd).paretoQ()
                paretoQSolution = paretoQSolution['Solution'].replace('', np.nan)
                paretoQSolution = paretoQSolution.dropna()
                var2 = var2.loc[paretoQSolution]
                var2 = var2['Result']
                
                newDf = pd.concat([var1, var2], axis = 1)
                newDf_column_name = newDf.columns.values
                newDf_column_name[0] = str(self.rankset1) #var1
                newDf_column_name[1] = str(self.rankset2) #var2

                newDf.columns = newDf_column_name
                newDf = newDf.fillna(1)
                var1 = newDf[str(self.rankset1)]
                var2 = newDf[str(self.rankset2)]
                
                table = pd.concat([var1, var2], axis = 1)
                print(table)