import json
import pandas as pd


def load_data(filepath1, filepath2):
    df1 = pd.read_csv(filepath1, sep="\t", header=0)

    df2 = pd.read_csv(filepath2, sep="\t", header=0)

    return df1, df2


def create_parts_dictionary():
    pass


def create_blocks():
    pass


def calculate_recall():
    pass


if __name__ == "__main__":
    df1, df2 = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
    )
    print(df1)
    print(df2)
    create_parts_dictionary()
    create_blocks()
    calculate_recall()
