import json
import pandas as pd
from jaro import jaro_winkler_metric


def load_data(filepath1, filepath2):
    df1 = pd.read_csv(filepath1, sep="\t", header=0)

    df2 = pd.read_csv(filepath2, sep="\t", header=0)

    return df1, df2


def create_parts_dictionary(df):
    name_parts_indexes = {}
    # NOTE name_parts_indexes will map a name part to a set of indexes of all the records with that name part
    # so if we do name_parts_indexes["Emil"] we get the set of indexes of all records that have "Emil" as a name part
    # Most sets will probably have just a single entry, but some names are much more common than others!
    for index, row in df.iterrows():
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            print("Error decoding name parts. Skipping.")
            continue
        for name_part in name_parts:
            name_parts_indexes.update(
                {name_part: (name_parts_indexes.get(name_part, set()).union({index}))}
            )
    return name_parts_indexes


# print(df.iloc[2475])  <- indexes dataframes by integer value


def create_blocks(df, name_parts_indexes, similarity_threshold=0.7):
    # given a dataset which *wasn't* used to create name_parts_indexes, blocks for each record
    # unfortunately, since transliteration doesn't produce the exact equivalent names, we can't just lookup our name parts the name_parts_indexes,
    # so we iterate over the keys and test for similarity instead.
    # this will take a while, but that's fine: Blocks only have to be made once.
    blocks = {}
    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        print(f"Blocking record {index}")
        blocks.update({index: set()})
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            print("Error decoding name parts. Skipping.")
            continue
        for name_part in name_parts:
            for key in name_parts_indexes:
                if jaro_winkler_metric(name_part, key) >= similarity_threshold:
                    # if the name_part is close enough to the key, union the current block with the new possible matches
                    possible_matches = name_parts_indexes[key]
                    blocks.update({index: (blocks.get(index)).union(possible_matches)})
    return blocks


def calculate_recall():
    pass


if __name__ == "__main__":
    df1, df2 = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
    )

    name_parts_indexes = create_parts_dictionary(df1)

    # Note that the size of a set is equivalent to the support of the name_part that maps to it, as defined in the Yad Vashem-paper
    print(
        f"Biggest set size: {max([len(item) for item in name_parts_indexes.values()])}"
    )

    blocks = create_blocks(df2, name_parts_indexes)

    print(f"Biggest block size: {max([len(item) for item in blocks.values()])}")
    print(f"Smallest block size: {min([len(item) for item in blocks.values()])}")

    calculate_recall()
