import json
import pandas as pd
from strsimpy.jaro_winkler import JaroWinkler


def load_data(filepath1, filepath2, matches_path):
    df1 = pd.read_csv(filepath1, sep="\t", header=0)

    df2 = pd.read_csv(filepath2, sep="\t", header=0)

    matches = pd.read_csv(matches_path, sep="\t", header=0)

    return df1, df2, matches


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
            print(f"Error decoding name parts. Skipping record {index}")
            continue
        for name_part in name_parts:
            name_parts_indexes.update(
                {name_part: (name_parts_indexes.get(name_part, set()).union({index}))}
            )
    # Note that the size of a set is equivalent to the support of the name_part that maps to it, as defined in the Yad Vashem-paper
    print(
        f"Biggest set size: {max([len(item) for item in name_parts_indexes.values()])}"
    )
    return name_parts_indexes


# print(df.iloc[2475])  <- indexes dataframes by integer value


def create_blocks_with_set_union(df, blocks_df, similarity_threshold=0.55):
    # given a dataset df to create blocks for, a dataset blocks_df to create blocks from, create blocks for each record in df
    # unfortunately, since transliteration doesn't produce the exact equivalent names, we can't just lookup our name parts the name_parts_indexes,
    # so we iterate over the keys and test for similarity instead.
    # this will take a while, but that's fine: Blocks only have to be made once.

    name_parts_indexes = create_parts_dictionary(blocks_df)

    blocks = {}
    block_size_sum = 0
    block_count = 0
    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        block_count += 1
        print(
            f"Blocking record {index}. Avg block size: {block_size_sum/block_count}",
            end="\r",
        )
        blocks.update({index: set()})
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            print(
                f"Skipped record {index} due to bad name parts.                                "
            )
            continue
        for name_part in name_parts:
            for key in name_parts_indexes:
                if JaroWinkler().similarity(name_part, key) >= similarity_threshold:
                    # if the name_part is close enough to the key, union the current block with the new possible matches
                    possible_matches = name_parts_indexes[key]
                    blocks.update({index: (blocks.get(index)).union(possible_matches)})
        block_size_sum += len(blocks[index])
    print("\n")
    return blocks


def create_blocks_with_part_scores(df, blocks_df, block_size=400):
    # instead of using a set union, assign a score to each record based on the most similar (or least distant) name part from some target record,
    # and then place the n records with the best score in the block for the target record, with n = block_size.

    name_parts_indexes = create_parts_dictionary(blocks_df)

    blocks = {}

    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Blocking record {index}",
            end="\r",
        )
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            print(
                f"Skipped record {index} due to bad name parts.                                "
            )
            continue
        # scoreboard will map records to scores which we use to find out what to include in our blocks
        scoreboard = {}
        for name_part in name_parts:
            for key in name_parts_indexes:
                score = JaroWinkler().similarity(name_part, key)
                for record in name_parts_indexes[key]:
                    # only update the scoreboard if the new score is greater than the score that was there already
                    scoreboard.update({record: max(scoreboard.get(record, 0), score)})
        # now sort the scoreboard records by score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard, key=lambda record: scoreboard[record], reverse=True
        )[:block_size]
        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def create_blocks_with_normalized_scores(
    # the same idea as create_blocks_with_part_scores, except we add the score instead of taking the max of the new and current score,
    # and then normalize all the scores afterwards based on the maximum possible score for each name part
    # the maximum score for a name part defaults to 1 but in the future, a callable could be used to calculate a max score dynamically
    df,
    blocks_df,
    block_size=400,
):

    name_parts_indexes = create_parts_dictionary(blocks_df)

    blocks = {}

    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Blocking record {index}",
            end="\r",
        )
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            print(
                f"Skipped record {index} due to bad name parts.                                "
            )
            continue
        # scoreboard will map records to scores which we use to find out what to include in our blocks
        scoreboard = {}
        # comparisons will map records to the number of comparisons we did to find the record's score
        comparisons = {}
        for name_part in name_parts:
            for key in name_parts_indexes:
                score = JaroWinkler().similarity(name_part, key)
                for record in name_parts_indexes[key]:
                    # only update the scoreboard if the new score is greater than the score that was there already
                    scoreboard.update({record: (scoreboard.get(record, 0) + score)})
                    comparisons.update({record: (comparisons.get(record, 0) + 1)})
        # we've filled out the scoreboard, so now we normalize it
        for record in scoreboard:
            max_score = 0
            # for record_name_part in json.loads(blocks_df.iloc[record]["name_parts"]).values():
            for _ in range(comparisons[record]):
                max_score += 1  # NOTE this is where the maximum possible score for each name part goes!
            scoreboard.update({record: (scoreboard.get(record) / max_score)})

        # now sort the scoreboard records by score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard, key=lambda record: scoreboard[record], reverse=True
        )[:block_size]
        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def calculate_recall(blocks, df1, df2, matches):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and a dataset that shows the matches between df1 and df2, calculate recall.
    total_matches = len(matches)
    found_matches = 0
    current_block = 0
    for df2_index, set in blocks.items():
        print(f"Finding recall for block {current_block}", end="\r")
        current_block += 1
        for df1_index in set:
            df1_name_parts = df1.iloc[df1_index]["name_parts"]
            df2_name_parts = df2.iloc[df2_index]["name_parts"]

            # FIXME hardcoding the columns here. Beware!
            is_match = (
                matches[matches["name_parts_roman"] == df1_name_parts][
                    "name_parts_LASKI"
                ]
                == df2_name_parts
            )
            sum = is_match.sum()
            if sum > 0:
                # NOTE did you know that there's a match that occurs twice? Look for "David Davidov" and "dvyd davydov" in transliterated_em.csv. That's why we add the sum and not just 1
                found_matches += sum
    recall = found_matches / total_matches
    print("\n")
    return recall


# TODO this alternative recall-finding method should be a lot faster, but it requires the matches-dataset to keep track of the IDs of the matching records instead of just their name parts!
# REVIEW this method gives a better recall than the other one
def calculate_recall_better(blocks, matches):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and a dataset that shows the matches between df1 and df2, calculate recall.
    total_matches = len(matches)
    found_matches = 0
    match_blocks = {}
    # match_blocks maps records from one dataset to the records from the other dataset they have a confirmed match with
    # in the same "order" as the blocks i.e. we want to map records from the dataset we made blocks for to records from the dataset we made blocks with
    for _, match in matches.iterrows():
        # FIXME hardcoded column names isn't the greatest
        match_blocks.update(
            {
                match["index_LASKI"]: match_blocks.get(
                    match["index_LASKI"], set()
                ).union({match["index_roman"]})
            }
        )

    for matched_record in match_blocks:
        for actual_match in match_blocks[matched_record]:
            print(
                f"Looking for match ({matched_record}, {actual_match}).     ",
                end="\r",
            )
            if actual_match in blocks[matched_record]:
                found_matches += 1

    recall = found_matches / total_matches
    print(f"\nFound {found_matches}/{total_matches} matches.\n")
    return recall


if __name__ == "__main__":
    df2, df1, matches = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
    )
    # df2 is LASKI, df1 is Zylbercweig

    blocks = {}
    try:
        with open(r"app\blocks.json") as file:
            print("Retrieving blocks...")
            blocks = json.load(file)
            blocks = {int(k): set(v) for k, v in blocks.items()}
    except OSError:  # NOTE we only do blocking if a blocks.json file doesn't exist!
        blocks = create_blocks_with_part_scores(df2, df1)
        # when we're done blocking, write the blocks to blocks.json. We must store our sets as lists due to the format
        blocks = {k: list(v) for k, v in blocks.items()}
        with open(r"app\blocks.json", "w", encoding="utf-8") as file:
            json.dump(blocks, file, ensure_ascii=False, indent=4)

    print(f"Biggest block size: {max([len(item) for item in blocks.values()])}")
    print(f"Smallest block size: {min([len(item) for item in blocks.values()])}\n")

    recall = calculate_recall_better(blocks, matches)
    print(f"Recall: {recall}")
