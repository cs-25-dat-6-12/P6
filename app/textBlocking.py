import json
import pandas as pd
import winsound
import numpy as np
from scipy.optimize import linear_sum_assignment
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


def create_parts_count_dictionary(name_parts_indexes):
    # given the dictionary created by create_parts_dictionary, returns a dictionary that maps records to the number of name parts in them
    parts_count = {}
    for name_part in name_parts_indexes:
        for record in name_parts_indexes[name_part]:
            parts_count.update({record: parts_count.get(record, 0) + 1})
    return parts_count


def create_blocks_with_set_union(df, blocks_df, similarity_threshold=1):
    # given a dataset df to create blocks for, a dataset blocks_df to create blocks from, create blocks for each record in df
    # unfortunately, since transliteration doesn't produce the exact equivalent names, we can't just lookup our name parts the name_parts_indexes,
    # so we iterate over the keys and test for similarity instead.
    # this will take a while, but that's fine: Blocks only have to be made once.

    name_parts_indexes = create_parts_dictionary(blocks_df)

    blocks = {}
    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Blocking record {index}",
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
    print("\n")
    return blocks


def create_blocks_with_part_scores(df, blocks_df, block_size=400, is_same_df=False):
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
        # make sure all records have a score in the scoreboard in order to guarantee block size
        for record in list(blocks_df.index):
            if scoreboard.get(record, None) == None:
                scoreboard.update({record: 0})
        # if the two datasets are the same, remove from the scoreboard the index of the record we are blocking for
        if is_same_df:
            scoreboard.pop(index, None)
        # now sort the scoreboard records by score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard, key=lambda record: scoreboard[record], reverse=True
        )[:block_size]
        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def create_blocks_with_normalized_scores(
    df, blocks_df, block_size=400, is_same_df=False
):
    # the same idea as create_blocks_with_part_scores, except we add the score instead of taking the max of the new and current score,
    # and then normalize all the scores afterwards based on the maximum possible score for each name part
    # the maximum score for a name part defaults to 1 but in the future, a callable could be used to calculate a max score dynamically

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
        # make sure all records have a score in the scoreboard in order to guarantee block size
        for record in list(blocks_df.index):
            if scoreboard.get(record, None) == None:
                scoreboard.update({record: 0})
        # we've filled out the scoreboard, so now we normalize it
        for record in scoreboard:
            max_score = 0
            # for record_name_part in json.loads(blocks_df.iloc[record]["name_parts"]).values():
            for _ in range(comparisons[record]):
                max_score += 1  # NOTE this is where the maximum possible score for each name part goes!
            scoreboard.update({record: (scoreboard.get(record) / max_score)})
        # if the two datasets are the same, remove from the scoreboard the index of the record we are blocking for
        if is_same_df:
            scoreboard.pop(index, None)
        # now sort the scoreboard records by score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard, key=lambda record: scoreboard[record], reverse=True
        )[:block_size]
        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def create_blocks_with_normalized_scores_revised(
    df, blocks_df, block_size=100, is_same_df=True
):
    # a revision of create_blocks_with_normalized_scores with the intention of getting closer to the original idea: Instead of summing up the maximum score for each name part,
    # we try to find the maximum score possible for disjoint pairs of name parts i.e. if we have "Emil Larsen" and "Emilie Larson",
    # then we only sum up the similarity of the pair "Emil" and "Emilie" and the pair "Larsen" and "Larson" (since those pairs maximize the sum of pairs' similarities),
    # and then divide by the number of pairs to normalize the score. Thus, we essentially have to solve an "Assignment Problem" for each pair of records from df and blocks_df.

    name_parts_indexes = create_parts_dictionary(blocks_df)
    parts_count = create_parts_count_dictionary(name_parts_indexes)

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
        # similarities will map records to an list of similarity scores which we concatenate as we find them.
        # If len(name_parts) = n and there are m name parts in a record, then the list can be turned into a matrix of shape n x m
        similarities = {}
        for name_part in name_parts:
            for key in name_parts_indexes:
                score = JaroWinkler().similarity(name_part, key)
                for record in name_parts_indexes[key]:
                    # append the score to the list
                    similarities.update(
                        {record: similarities.get(record, list()) + [score]}
                    )
        # with all the similarities lists filled out, we find the best combination of name part pairs and calculate a score for each record
        for record in similarities:
            if len(name_parts) > 0 and parts_count[record] > 0:
                array = np.array(similarities[record])
                matrix = array.reshape(len(name_parts), parts_count[record])
                row_indexes, col_indexes = linear_sum_assignment(matrix, maximize=True)
                # a record's score is the sum of the matrix elements that maximize the Assignment Problem value divided by the number of elements chosen
                scoreboard.update(
                    {record: matrix[row_indexes, col_indexes].sum() / len(row_indexes)}
                )
            else:
                scoreboard.update({record: 0})
        # make sure all records have a score in the scoreboard in order to guarantee block size
        for record in list(blocks_df.index):
            if scoreboard.get(record, None) == None:
                scoreboard.update({record: 0})
        # if the two datasets are the same, remove from the scoreboard the index of the record we are blocking for
        if is_same_df:
            scoreboard.pop(index, None)
        # now sort the scoreboard records by score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard, key=lambda record: scoreboard[record], reverse=True
        )[:block_size]
        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def create_blocks_with_threshold_scores(
    df, blocks_df, block_size=300, similarity_threshold=1, is_same_df=True
):
    # the same idea as create_blocks_with_set_union, except we add a point to the relevant records instead of joining them with the block
    # afterwards we normalize the scores by dividing each record's score with the number of name parts in the records
    # and then place the n records with the best score in the block for the target record, with n = block_size.
    # if the two datasets are the same, we remove any mapping from an index to the same index, since that's a trivial match

    name_parts_indexes = create_parts_dictionary(blocks_df)
    parts_count = create_parts_count_dictionary(name_parts_indexes)

    blocks = {}

    for index, row in df.iterrows():
        # a block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Blocking record {index}",
            end="\r",
        )
        blocks.update({index: set()})
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            blocks.update({index: set()})
            print(
                f"Skipped record {index} due to bad name parts.                                "
            )
            continue
        # scoreboard will map records to scores which we use to find out what to include in our blocks
        scoreboard = {}
        for name_part in name_parts:
            for key in name_parts_indexes:
                if JaroWinkler().similarity(name_part, key) >= similarity_threshold:
                    # if the name_part is close enough to the key, union the current block with the new possible matches
                    for record in name_parts_indexes[key]:
                        scoreboard.update({record: scoreboard.get(record, 0) + 1})
        # make sure all records have a score in the scoreboard in order to guarantee block size
        for record in list(blocks_df.index):
            if scoreboard.get(record, None) == None:
                scoreboard.update({record: 0})
            # and remove records from the scoreboard if they have no name parts
            if parts_count.get(record, None) == None:
                scoreboard.pop(record, None)
        # if the two datasets are the same, remove from the scoreboard the index of the record we are blocking for
        if is_same_df:
            scoreboard.pop(index, None)
        # now sort the scoreboard records by normalized score and put the n best records in the block with n = block_size
        # NOTE if distance is used as score instead of a similarity, simply let reverse=False instead of reverse=True
        best_records = sorted(
            scoreboard,
            key=lambda record: (scoreboard[record] / parts_count[record]),
            reverse=True,
        )[:block_size]

        blocks.update({index: set(best_records)})
    print("\n")
    return blocks


def create_match_blocks(matches):
    # given a dataset with confirmed matching name pairs, create the smallest possible blocks to contain these pairs.
    # In other words: create blocks that only contain matches (or any other kind of pair given some column names)
    keys_column = "index_LASKI"
    values_column = "index_roman"
    match_blocks = {}
    # match_blocks will be a dict where values from the "keys_column" will map to values from the "values_column"

    for _, match in matches.iterrows():
        # FIXME hardcoded column names isn't the greatest
        match_blocks.update(
            {
                match[keys_column]: match_blocks.get(match[keys_column], set()).union(
                    {match[values_column]}
                )
            }
        )

    return match_blocks


def calculate_recall_better(blocks, matches):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and a dataset that shows the matches between df1 and df2, calculate recall.
    total_matches = len(matches)
    found_matches = 0
    match_blocks = create_match_blocks(matches)
    # match_blocks maps records from one dataset to the records from the other dataset they have a confirmed match with
    # in the same "order" as the blocks i.e. we want to map records from the dataset we made blocks for to records from the dataset we made blocks with

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


def calculate_precision(blocks, matches):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and a dataset that shows the matches between df1 and df2, calculate precision.
    possible_matches = 0
    found_matches = 0
    match_blocks = create_match_blocks(matches)
    # match_blocks maps records from one dataset to the records from the other dataset they have a confirmed match with
    # in the same "order" as the blocks i.e. we want to map records from the dataset we made blocks for to records from the dataset we made blocks with

    for matched_record in match_blocks:
        for actual_match in match_blocks[matched_record]:
            print(
                f"Looking for match ({matched_record}, {actual_match}).     ",
                end="\r",
            )
            if actual_match in blocks[matched_record]:
                found_matches += 1

    for block in blocks.values():
        possible_matches += len(block)

    precision = found_matches / possible_matches
    print(f"\n{found_matches}/{possible_matches} identified matches are correct.\n")
    return precision


def calculate_reduction_ratio(blocks, df1, df2):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and the datasets the blocks were made from df1 and df2, calculate the reduction ratio.
    blocked_comparisons = 0
    for block in blocks.values():
        blocked_comparisons += len(block)
    brute_force_comparisons = len(df1) * len(df2)
    reduction_ratio = 1 - (blocked_comparisons / brute_force_comparisons)
    print(f"Reduced {brute_force_comparisons} comparisons to {blocked_comparisons}")
    return reduction_ratio


if __name__ == "__main__":
    df2, df1, matches = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
    )

    blocks = {}
    try:
        with open(r"app\blocks.json") as file:
            print("Retrieving blocks...")
            blocks = json.load(file)
            blocks = {int(k): set(v) for k, v in blocks.items()}
    except OSError:  # NOTE we only do blocking if a blocks.json file doesn't exist!
        try:
            blocks = create_blocks_with_part_scores(df2, df1)
        except Exception as e:
            # beep with frequency 1000 for 1000 ms if something goes wrong during blocking
            winsound.Beep(1000, 1000)
            raise e
        # when we're done blocking, write the blocks to blocks.json. We must store our sets as lists due to the format
        blocks = {k: list(v) for k, v in blocks.items()}
        with open(r"app\blocks.json", "w", encoding="utf-8") as file:
            json.dump(blocks, file, ensure_ascii=False, indent=4)
        # beep with frequency 2500 for 1000 ms when blocking is done
        winsound.Beep(1500, 1000)
    print(f"Biggest block size: {max([len(item) for item in blocks.values()])}")
    print(f"Smallest block size: {min([len(item) for item in blocks.values()])}\n")

    recall = calculate_recall_better(blocks, matches)
    print(f"Recall: {recall}")
    reduction_ration = calculate_reduction_ratio(blocks, df1, df2)
    print(f"Reduction ratio: {reduction_ration}")
