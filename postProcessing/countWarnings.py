""" Program to count warnings for each asset and statistic

    Progrom counts warnings for each asset and statistic
    and save countings in text file 'countings.txt
"""
__author__ = "Krzysztof Łapan, Maciej Morawski"

import load as load
import pandas as pd

def getSpecficData(df, stat, asset):
    df_stat = df[df["stat"] == stat]
    return df_stat[df_stat["assetNo"] == asset]

def main():
    database = load.loadFile("output.txt")
    statistics = ["mean", "median", "10th quantile", "mean of 10% smallest", "security measure 1", "security measure 2"]
    df = database.getData()
    content = []
    for stat in statistics:
        for asset in range(7):
            specificData = getSpecficData(df, stat, asset)
            content.append([stat, asset, len(specificData.index)])

    countings = pd.DataFrame(content)
    countings.columns = ["stat", "assetNo", "value"]
    
    sourceFile = open('countings.txt', 'w')
    print(countings.to_string(index=False), file = sourceFile)
    sourceFile.close


if __name__ == '__main__':
    main()
