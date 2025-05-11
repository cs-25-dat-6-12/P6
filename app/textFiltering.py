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


def filter_with_set_union(blocks, df, blocks_df, similarity_threshold=0.65):
    # given pairwise comparison blocks which map records from a dataset df to records from a dataset blocks_df, create new pairwise comparison blocks:
    # Since transliteration doesn't produce the exact equivalent names, we can't just lookup our name parts in other records,
    # so we iterate over the keys in name_parts_indexes and test for similarity instead.
    # When similarity is above a set threshold, add all records with that key to the comparison blocks for the current record

    name_parts_indexes = create_parts_dictionary(blocks_df)

    filtered_blocks = {}
    for index, row in df.iterrows():
        # a comparison block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Filtering record {index}",
            end="\r",
        )
        filtered_blocks.update({index: set()})
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
                    # if the name_part is close enough to the key, union the current block with the new possible matches.
                    # NOTE possible matches are only included if they're already in the record's block! Hence the intersection with blocks[index].
                    possible_matches = name_parts_indexes[key]
                    filtered_blocks.update(
                        {
                            index: (filtered_blocks.get(index)).union(
                                possible_matches.intersection(blocks[index])
                            )
                        }
                    )
    print("\n")
    return filtered_blocks


def filter_with_part_scores(df, blocks_df, block_size=200, is_same_df=False):
    # instead of using a set union, assign a score to each record based on the most similar (or least distant) name part from some target record,
    # and then place the n records with the best score in the block for the target record, with n = block_size.

    name_parts_indexes = create_parts_dictionary(blocks_df)

    filtered_blocks = {}

    for index, row in df.iterrows():
        # a comparison block is an index of a record and a set of all the indexes of records that it might match with
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
        # scoreboard will map records to scores which we use to find out what to include in our filtered blocks
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
        filtered_blocks.update({index: set(best_records)})
    print("\n")
    return filtered_blocks


def filter_with_normalized_scores_revised(
    df, blocks_df, block_size=100, is_same_df=True
):
    # a revision of filter_with_normalized_scores with the intention of getting closer to the original idea: Instead of summing up the maximum score for each name part,
    # we try to find the maximum score possible for disjoint pairs of name parts i.e. if we have "Emil Larsen" and "Emilie Larson",
    # then we only sum up the similarity of the pair "Emil" and "Emilie" and the pair "Larsen" and "Larson" (since those pairs maximize the sum of pairs' similarities),
    # and then divide by the number of pairs to normalize the score. Thus, we essentially have to solve an "Assignment Problem" for each pair of records from df and blocks_df.

    name_parts_indexes = create_parts_dictionary(blocks_df)
    parts_count = create_parts_count_dictionary(name_parts_indexes)

    filtered_blocks = {}

    for index, row in df.iterrows():
        # a comparison block is an index of a record and a set of all the indexes of records that it might match with
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
        # scoreboard will map records to scores which we use to find out what to include in our filtered blocks
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
        filtered_blocks.update({index: set(best_records)})
    print("\n")
    return filtered_blocks


def filter_with_threshold_scores(
    df, blocks_df, block_size=300, similarity_threshold=1, is_same_df=True
):
    # the same idea as filter_with_set_union, except we add a point to the relevant records instead of joining them with the block
    # afterwards we normalize the scores by dividing each record's score with the number of name parts in the records
    # and then place the n records with the best score in the block for the target record, with n = block_size.
    # if the two datasets are the same, we remove any mapping from an index to the same index, since that's a trivial match

    name_parts_indexes = create_parts_dictionary(blocks_df)
    parts_count = create_parts_count_dictionary(name_parts_indexes)

    filtered_blocks = {}

    for index, row in df.iterrows():
        # a comparison block is an index of a record and a set of all the indexes of records that it might match with
        print(
            f"Blocking record {index}",
            end="\r",
        )
        filtered_blocks.update({index: set()})
        name_parts = []
        try:
            name_parts = json.loads(row["name_parts"]).values()
        except json.JSONDecodeError:
            filtered_blocks.update({index: set()})
            print(
                f"Skipped record {index} due to bad name parts.                                "
            )
            continue
        # scoreboard will map records to scores which we use to find out what to include in our filtered blocks
        scoreboard = {}
        for name_part in name_parts:
            for key in name_parts_indexes:
                if JaroWinkler().similarity(name_part, key) >= similarity_threshold:
                    # if the name_part is close enough to the key, add a point to all the records mapped to by that key
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

        filtered_blocks.update({index: set(best_records)})
    print("\n")
    return filtered_blocks


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
    print(f"\nFound {found_matches}/{total_matches} matches.")
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

    if possible_matches == 0:
        # this prevents a division by zero in case possible_matches is zero
        return 0
    precision = found_matches / possible_matches
    print(f"\n{found_matches}/{possible_matches} identified matches are correct.")
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


def find_missed_matches(blocks, matches):
    # given blocks as a dictionary in the form generated by create_blocks(),
    # where record IDs from df2 map to sets of record IDs from df1,
    # and a dataset that shows the matches between df1 and df2,
    # create blocks consisting of the matches that are not present in the blocks.
    match_blocks = create_match_blocks(matches)
    missed_matches = {}
    # match_blocks maps records from one dataset to the records from the other dataset they have a confirmed match with
    # in the same "order" as the blocks i.e. we want to map records from the dataset we made blocks for to records from the dataset we made blocks with

    for matched_record in match_blocks:
        for actual_match in match_blocks[matched_record]:
            print(
                f"Looking for match ({matched_record}, {actual_match}).     ",
                end="\r",
            )
            if actual_match not in blocks[matched_record]:
                missed_matches.update(
                    {
                        matched_record: missed_matches.get(matched_record, set()).union(
                            {actual_match}
                        )
                    }
                )
    return missed_matches


if __name__ == "__main__":
    df2, df1, matches = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
    )

    filtered_blocks = {}
    try:
        with open(r"app\filtered_blocks.json") as file:
            print("Retrieving filtered blocks...")
            filtered_blocks = json.load(file)
            filtered_blocks = {int(k): set(v) for k, v in filtered_blocks.items()}
    except (
        OSError
    ):  # NOTE we only do filtering if a filtered_blocks.json file doesn't exist!
        try:
            with open(r"app\blocks.json") as file:
                # load the blocks created in the blocking phase and make the lists into sets again
                blocks = json.load(file)
                blocks = {int(k): set(v) for k, v in blocks.items()}
                filtered_blocks = filter_with_set_union(blocks, df2, df1)
        except Exception as e:
            # beep with frequency 1000 for 1000 ms if something goes wrong during filtering
            winsound.Beep(1000, 1000)
            raise e
        # when we're done filtering, write the blocks to filtered_blocks.json. We must store our sets as lists due to the format
        filtered_blocks = {k: list(v) for k, v in filtered_blocks.items()}
        with open(r"app\filtered_blocks.json", "w", encoding="utf-8") as file:
            json.dump(filtered_blocks, file, ensure_ascii=False, indent=4)
        # beep with frequency 1500 for 1000 ms when blocking is done
        winsound.Beep(1500, 1000)
    print(
        f"Most pairwise comparisons for one name: {max([len(item) for item in filtered_blocks.values()])}"
    )
    print(
        f"Fewest pairwise comparisons for one name: {min([len(item) for item in filtered_blocks.values()])}\n"
    )

    recall = calculate_recall_better(filtered_blocks, matches)
    print(f"Recall: {recall}")
    reduction_ration = calculate_reduction_ratio(filtered_blocks, df1, df2)
    print(f"Reduction ratio: {reduction_ration}")
    missed_matches = find_missed_matches(filtered_blocks, matches)

    with open(r"app\filtering_missed_matches.tsv", "w") as file:
        file.write("df2_title\tdf2_name_parts\tdf1_title\tdf1_name_parts\n")
        for record in missed_matches:
            for missed_match in missed_matches[record]:
                df2_row = df2.iloc[record]
                df1_row = df1.iloc[missed_match]
                file.write(
                    f"{df2_row["title"]}\t{df2_row["name_parts"]}\t{df1_row["title"]}\t{df1_row["name_parts"]}\n"
                )
