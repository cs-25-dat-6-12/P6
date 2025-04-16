import json
import pandas as pd


def load_data(filepath1, filepath2):
    df1 = pd.read_csv(filepath1, sep="\t", header=0)

    df2 = pd.read_csv(filepath2, sep="\t", header=0)

    return df1, df2


def create_parts_dictionary(df):
    name_parts_indexes = {}
    biggest_set_size = 0
    # NOTE name_parts_indexes will map a name part to a set of indexes of all the records with that name part
    # so if we do name_parts_indexes["Emil"] we get the set of indexes of all records that have "Emil" as a name part
    # Most sets will probably have just a single entry, but some names are much more common than others!
    for index, row in df.iterrows():
        for name_part in json.loads(row["name_parts"]).values():
            name_parts_indexes.update(
                {name_part: (name_parts_indexes.get(name_part, set()).union({index}))}
            )
            # keep track of the biggest set for debug purposes.
            # Note that the size of a set is equivalent to the support of the name_part that maps to it, as defined in the Yad Vashem-paper
            biggest_set_size = max(
                biggest_set_size, len(name_parts_indexes.get(name_part))
            )
    print(f"Biggest set: {biggest_set_size}")
    return name_parts_indexes


# print(df.iloc[2475])  <- indexes dataframes by integer value


def create_blocks(df, name_parts_indexes):
    # given a dataset which *wasn't* used to create name_parts_indexes, create a set of indexes of possible matches for each record
    # unfortunately, since transliteration doesn't produce the exact equivalent names, we can't just lookup our name parts the name_parts_indexes,
    # so we iterate over the keys and test for similarity instead.
    # this will take a while, but that's fine: Blocks only have to be made once.
    blocks = {}
    for _, row in df.iterrows():
        for name_part in json.loads(row["name_parts"]).values():
            for name_part_key in name_parts_indexes:
                pass


def calculate_recall():
    pass


if __name__ == "__main__":
    df1, df2 = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
    )

    name_parts_indexes = create_parts_dictionary(df1)

    create_blocks(df2, name_parts_indexes)

    calculate_recall()
