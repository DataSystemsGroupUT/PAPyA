from PAPyA.Rank import SDRank, MDRank, RTA


class bestOfSD(SDRank):
    def __init__(self, config_path: str, log_path: str, size: str, sd: str, k: int):
        super().__init__(config_path, log_path, size, sd)
        self.k = k

    def bestOfSD(self):
        df = SDRank(self.config_path, self.log_path,
                    self.size, self.sd).calculateRank()
        df = df['Result']
        df = df.nlargest(self.k)
        return df.index.to_numpy().tolist()


class bestOfParetoQ(MDRank):
    def __init__(self, config_path: str, log_path: str, size: str, k: int, sd=None):
        super().__init__(config_path, log_path, size, sd)
        self.k = k

    def bestOfParetoQ(self):
        df = MDRank(self.config_path, self.log_path,
                    self.size, self.sd).paretoQ()
        df = list(df['Solution'])[:self.k]
        return df


class bestOfParetoAgg(MDRank):
    def __init__(self, config_path: str, log_path: str, size: str, k: int, sd=None):
        super().__init__(config_path, log_path, size, sd)
        self.k = k

    def bestOfParetoAgg(self):
        df = MDRank(self.config_path, self.log_path,
                    self.size, self.sd).paretoAgg()
        df = list(df['Solution'])[:self.k]
        return df


class bestOfRTA(RTA):
    def __init__(self, config_path: str, log_path: str, size: str, k: int, sd=None):
        super().__init__(config_path, log_path, size, sd)
        self.k = k

    def bestOfRTA(self):
        df = RTA(self.config_path, self.log_path, self.size, self.sd).rta()
        df = list(df.index)
        return df[:self.k]
