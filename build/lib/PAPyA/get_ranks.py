from .file_reader import FileReader
import pandas as pd
import scipy.stats as ss

class getRanks(FileReader):
    def __init__(self, config_path: str, log_path: str, size: str, sd=None):
        super().__init__(config_path, log_path, size, sd)
    
    def getRanks(self):
        load = FileReader(self.config_path, self.log_path, self.size, self.sd)
        df = load.file_reader()
        df_transpose = df.T
        df_full_ranks = []
        for index, row in df_transpose.iterrows():
            df_full_ranks.append(ss.rankdata(row, method = 'max'))
        df_full_ranks = pd.DataFrame(df_full_ranks)
        df_full_ranks = df_full_ranks.T
        df_full_ranks = df_full_ranks.set_axis(df_transpose.columns, axis = 'index') #configurations  
        df_full_ranks = df_full_ranks.set_axis([i+1 for i in range(len(df_transpose.index))], axis='columns') #query
        
        return df_full_ranks