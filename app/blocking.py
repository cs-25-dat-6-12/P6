from datetime import datetime
import json
import pandas as pd
from textFiltering import (
    calculate_recall_better,
    calculate_reduction_ratio,
    create_phonetic_name_parts,
)
from ipapy import is_valid_ipa
from ipapy import UNICODE_TO_IPA
from ipapy.ipachar import IPAConsonant
from itertools import combinations


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


def create_pairwise_comparison_blocks(
    dict_1, dict_2, is_same_df=False, ignore_lables=["None"]
):
    # given dictionaries produced by block_dataframe, create blocks in the following format:
    # A record from dict_1 maps to a set of the records from dict_2 it shares at least one block label with.
    # If is_same_df is set to True, records will not map to themselves.
    # ignore_lables is optional and contains a list of labels for which no pairwise comparisons should be created
    comparison_blocks = {}
    for label in dict_1:
        print(f'Creating comparison blocks from block "{label}"          ', end="\n")
        if label in ignore_lables:
            # NOTE even if the label should be ignored, we make sure the records there have a block to prevent errors later
            for record in dict_1[label]:
                comparison_blocks.update({record: comparison_blocks.get(record, set())})
        else:
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
        else:
            return ["None"]
    except json.JSONDecodeError:
        return ["None"]


def phonetic_consonant_presence(row):
    # return the IPA characters the row's phonetic encoding contains.
    # Requires ipapy to be installed! Install it with "pip install ipapy".
    phoneme = str(row["phoneme"])
    labels = set()
    if not is_valid_ipa(phoneme):
        # FIXME find out how to handle * in phonetic encodings so we don't have to throw it out
        phoneme = phoneme.replace("*", "")
    assert is_valid_ipa(phoneme)
    j = 0
    char_found = False
    ipa = ""
    # find the longest possible IPA characters we can find
    for i in range(len(phoneme)):
        try:
            ipa = UNICODE_TO_IPA[phoneme[j:i]]
            char_found = True
        except KeyError:
            # UNICODE_TO_IPA will raise a KeyError if the unicode character is invalid.
            # We use this to check whether or not the current IPA character is valid!
            if char_found:
                j = i
                if type(ipa) == IPAConsonant:
                    labels = labels.union({ipa})
                char_found = False
    if char_found and type(ipa) == IPAConsonant:
        labels = labels.union({ipa})
    if len(labels) < 1:
        labels = ["None"]
    return list(labels)


def name_length(row, slack=1):
    title = row["title"]
    length = len(title)
    labels = []
    # the labels are the length and all the integers within the slack
    # (we add +1 to the end of the range because range doesn't include the end)
    for i in range(length - slack, length + slack + 1):
        labels += [i]
    return labels


def potential_name_length(row, slack=3):
    try:
        name_parts = json.loads(row["name_parts"])
        part_lengths = [len(x) for x in name_parts.values()]
        possible_lengths = []
        for i in range(1, len(name_parts) + 1):
            adjusted_slack = slack * i
            for combination in combinations(part_lengths, i):
                for j in range(-adjusted_slack, adjusted_slack + 1):
                    possible_lengths.append(sum(combination) + j)
        return possible_lengths
    except json.JSONDecodeError:
        return ["None"]
    

def age_blocking(row, slack=0):
    age = row["age"]
    labels = []
    for i in range(age - slack, age + slack + 1):
        labels += [i]
    return labels



if __name__ == "__main__":
    df1 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.csv", sep=",", header=0
    )
    df2 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig.csv",
        sep=",",
        header=0,
    )

    labeler = no_distinguishing

    start_time = datetime.now()
    print("Blocking first dataset...")
    dict_1 = block_dataframe(df1, labeler)
    print("Blocking second dataset...")
    dict_2 = block_dataframe(df2, labeler)
    end_time = datetime.now()

    print("Preparing pairwise comparisons...")
    blocks = create_pairwise_comparison_blocks(dict_1, dict_2, is_same_df=False)

    print("Evaluating blocks...\n")
    matches = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
        sep="\t",
        header=0,
    )
    print(f"Time taken: {(end_time-start_time).total_seconds()} seconds.")
    recall = calculate_recall_better(blocks, matches)
    print(f"Recall: {recall}\n")
    reduction_ration = calculate_reduction_ratio(blocks, df1, df2)
    print(f"Reduction ratio: {reduction_ration}\n")

    print("Writing blocks to file...")
    blocks = {k: list(v) for k, v in blocks.items()}
    with open(r"app\blocks.json", "w", encoding="utf-8") as file:
        json.dump(blocks, file, ensure_ascii=False, indent=4)
    print("All done and ready for filtering!")
