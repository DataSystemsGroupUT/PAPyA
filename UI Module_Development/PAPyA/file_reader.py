from .config_loader import Loader
from .joinTuple import joinTuple
import itertools
import pandas as pd


class FileReader(Loader, joinTuple):
    def __init__(self, config_path: str, log_path: str, size: str, sd: str, joined_string=None):
        super().__init__(config_path)
        self.log_path = log_path
        self.size = size
        self.sd = sd
        self.joined_string = joined_string

    def file_reader(self, args=None):
        loader = Loader(self.config_path)
        data = loader.loader()
        d = data.get('dimensions')
        options = list(d.values())
        query = data.get('query')

        tuple = list(itertools.product(*options))
        self.joined_string = map(joinTuple.join_tuple_string, tuple)
        self.joined_string = list(self.joined_string)

        avg = []
        for i in self.joined_string:
            df = pd.read_csv(
                f'{self.log_path}/{self.size}/{i}.txt', sep=',', header=None)
            df = df.fillna(0)
            mean = df.mean(axis=0, numeric_only=True)
            avg.append(mean)

        df = pd.DataFrame(avg, index=self.joined_string)
        df = df.set_axis(["Q"+str(i+1) for i in range(query)], axis=1)
        # max_value = df.max(numeric_only=True).max()
        # df = df.fillna(df.max())
        max_value = df.max()
        max_value = max_value.apply(lambda x: x*(11/10))
        df = df.fillna(max_value)

        if self.sd != list(d.keys())[-1]:
            li = []
            config = list(df.index)
            d_list = list(d)
            key_slice = list(itertools.islice(
                d_list, d_list.index(self.sd)+1, None))
            number_of_options = [d[x]for x in key_slice]
            total_length = len(list(itertools.product(*number_of_options)))

            for counter in range(total_length):
                for x in range(int(len(config)/total_length)):
                    li.append(config[counter])
                    counter = counter+total_length
            df = df.reindex(index=li)

        if args == None:
            return df

        elif args != None:
            options = []
            store = []
            for arg in args:
                if isinstance(arg, str):
                    if arg not in d.get(self.sd):
                        options.append(arg)
                    elif arg in d.get(self.sd):
                        raise Exception(
                            "Options cannot be in the choosen dimension")
                else:
                    pass
            for x in range(len(options)):
                c = fr'(?=.*\b{options[x]}\b)'
                store.append(c)
            command = "".join(store)
            df = df.loc[df.index.str.contains(command, regex=True)]
            return df
