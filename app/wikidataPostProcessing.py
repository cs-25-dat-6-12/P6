# this is the total number of matches in the wikidata-set.
# It's hardcoded for now so we don't have to find it every time, and it doesn't change, so that's ok.
import json
import pandas as pd

brute_force_comparisons = 11699820
total_matches = 3466


def countMatches(df, blocks):
    matches = 0
    non_matches = 0
    for record in blocks:
        print(f"Counting matches in block {record}    ", end="\r")
        for possible_match in blocks[record]:
            if df.iloc[record]["id"] == df.iloc[possible_match]["id"]:
                matches += 1
            else:
                non_matches += 1
    print("")
    return matches, non_matches


def calculate_wikidata_recall_and_precision(df, blocks):
    true_positives, false_positives = countMatches(df, blocks)

    recall = true_positives / total_matches
    precision = true_positives / (true_positives + false_positives)
    print(
        f"Combined block size: {true_positives + false_positives}\nCorrect matches: {true_positives}\nIncorrect matches: {false_positives}\nRecall: {recall}\nPrecision: {precision}"
    )

    return recall, precision


def getWikidataDf():
    df = pd.read_csv(
        r"datasets\phonetic\wikiData-title-nameparts\wikiData_merged_phonetic.csv",
        sep=",",
        header=0,
    )
    mask = df["birth"].apply(lambda x: x.startswith("1725"))
    mask += df["birth"].apply(lambda x: x.startswith("1726"))
    mask += df["birth"].apply(lambda x: x.startswith("1728"))
    print(len(df.index))
    df = df[mask]
    print(len(df.index))
    return df.reset_index()


if __name__ == "__main__":
    df = getWikidataDf()
    with open(r"app\blocks.json", "r") as blocks_file:
        blocks = json.load(blocks_file)
        blocks = {int(k): set(v) for k, v in blocks.items()}
    recall, precision = calculate_wikidata_recall_and_precision(df, blocks)
    f1 = 0
    if (precision + recall) != 0:
        f1 = 2 * (precision * recall) / (precision + recall)
    print(f"F1: {f1}")
