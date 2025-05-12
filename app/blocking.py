import json
import pandas as pd
from textFiltering import calculate_recall_better, calculate_reduction_ratio


def block_dataframe(df, callable):
    # given a dataframe and a callable that returns a list of block labels when given a pd.Series representing a row,
    # create a dictionary that maps block labels to a list of indexes of the records in those blocks
    blocks = {}
    for index, row in df.iterrows():
        print(f"Finding blocks for record {index} ", end="\r")
        assert df.iloc[index].equals(row)
        labels = callable(row)
        for label in labels:
            blocks.update({label: blocks.get(label, list()) + [index]})
    print("")
    return blocks


def create_pairwise_comparison_blocks(dict_1, dict_2, is_same_df=False):
    # given dictionaries produced by block_dataframe, create blocks in the following format:
    # A record from dict_1 maps to a set of the records from dict_2 it shares at least one block label with.
    # If is_same_df is set to True, records will not map to themselves.
    comparison_blocks = {}
    for label in dict_1:
        print(f'Creating comparison blocks from block "{label}"          ', end="\r")
        for record in dict_1[label]:
            comparison_blocks.update(
                {
                    record: comparison_blocks.get(record, set()).union(
                        set(dict_2.get(label, list()))
                    )
                }
            )
    print("")
    if is_same_df:
        # NOTE if the dicts come from the same place, then we don't want to waste time matching records with themselves
        for record in comparison_blocks:
            print(f"Cleaning comparison block {record}    ", end="\r")
            comparison_blocks[record].discard(record)
        print("")
    return comparison_blocks


# ANCHOR the functions defined below are all callables for use by block_dataframe.
# They should all return iterables and their input should always be a pd.Series.


def example_callable(row):
    # return odd or even depending on the length of the title
    title = row["title"]
    if len(title) % 2:
        return ["odd"]
    else:
        return ["even"]


def no_distinguishing(row):
    # return the same label no matter what. The same as not blocking at all
    return ["singleton_block"]


def name_part_presence(row):
    # return the types of name parts the name has, or "None" if it has none
    try:
        name_parts = json.loads(row["name_parts"]).keys()
        if len(name_parts) > 0:
            return name_parts
    except json.JSONDecodeError:
        pass
    finally:
        return ["None"]


def name_length(row, slack=4):
    title = row["title"]
    length = len(title)
    labels = []
    # the labels are the length and all the integers within the slack
    # (we add +1 to the end of the range because range doesn't include the end)
    for i in range(length - slack, length + slack + 1):
        labels += [i]
    return labels


if __name__ == "__main__":
    df1 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv", sep="\t", header=0
    )
    df2 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig.tsv",
        sep="\t",
        header=0,
    )

    labeler = no_distinguishing

    print("Blocking first dataset...")
    dict_1 = block_dataframe(df1, labeler)
    print("Blocking second dataset...")
    dict_2 = block_dataframe(df2, labeler)

    print("Preparing pairwise comparisons...")
    blocks = create_pairwise_comparison_blocks(dict_1, dict_2, is_same_df=False)

    print("Evaluating blocks...\n")
    matches = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
        sep="\t",
        header=0,
    )
    recall = calculate_recall_better(blocks, matches)
    print(f"Recall: {recall}\n")
    reduction_ration = calculate_reduction_ratio(blocks, df1, df2)
    print(f"Reduction ratio: {reduction_ration}\n")

    print("Writing blocks to file...")
    blocks = {k: list(v) for k, v in blocks.items()}
    with open(r"app\blocks.json", "w", encoding="utf-8") as file:
        json.dump(blocks, file, ensure_ascii=False, indent=4)
    print("All done and ready for filtering!")
