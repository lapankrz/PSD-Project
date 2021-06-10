""" Loading data class
 
    Implementation of loading data from text file with warnings
    generated in stream processing unit
"""
__author__ = "Krzysztof Łapan, Maciej Morawski"

import pandas as pd

class loadFile:
    def __init__(self, filename = "output.txt"):
        self.filename = filename
        self.load()

    def split_line(self, line):
        values = line[1:-2].split(",")
        return [values[1], int(values[2]), float(values[3])]

    def load(self):
        with open(self.filename) as f:
            content = f.readlines()

        content = [self.split_line(x.strip()) for x in content]

        self.df = pd.DataFrame(content)
        self.df.columns = ["stat", "assetNo", "value"]

    def getData(self):
        return self.df