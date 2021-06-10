""" Program to create distribution plots for each asset and statistic

    Progrom creates distribution plots for each asset and statistic
    and save them in image files
"""
__author__ = "Krzysztof Łapan, Maciej Morawski"

import load as load
import pandas as pd
import matplotlib.pyplot as plt

def getSpecficData(df, stat, asset):
    df_stat = df[df["stat"] == stat]
    return df_stat[df_stat["assetNo"] == asset]

def main():
    database = load.loadFile("output.txt")
    statistics = ["mean", "median", "10th quantile", "mean of 10% smallest", "security measure 1", "security measure 2"]
    df = database.getData()

    specific_plots = True
    grouped_plots = True

    if specific_plots:
        for stat in statistics:
            for asset in range(7):
                specificData = getSpecficData(df, stat, asset)
                specificData.value.plot.hist(bins = 10)
                if asset == 6:
                    plt.title("{} for whole wallet".format(stat))
                else:
                    plt.title("{} for asset {} alerts".format(stat, asset+1))
                plt.xlabel('{}'.format(stat))
                plt.savefig("plots_specific/{}_{}.png".format(stat.strip(), asset))
                plt.clf()

    if grouped_plots:
        labels = ["Asset 1", "Asset 2", "Asset 3", "Asset 4", "Asset 5", "Asset 6", "Whole wallet"]
        for stat in statistics:
            for asset in range(7):
                specificData = getSpecficData(df, stat, asset)
                specificData.value.plot.hist(bins = 10)
            plt.xlabel('{}'.format(stat))
            plt.title("{} alerts".format(stat))
            plt.legend(labels)
            plt.savefig("plots_stats/{}s.png".format(stat.strip()))
            plt.clf()

if __name__ == '__main__':
    main()
