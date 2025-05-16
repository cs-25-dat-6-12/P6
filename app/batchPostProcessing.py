import json

import pandas as pd
from textFiltering import (
    calculate_recall_better,
    calculate_precision,
    find_missed_matches,
)


def load_response_booleans(output_filepath):
    # given the path of a jsonl file produced as output to a batch job,
    # create a dictionary that maps records to their outputs, parsed as booleans
    with open(output_filepath) as output_file:
        output_booleans = {}
        for line in output_file:
            line = json.loads(line)
            # this gets the (boolean) content of the model response for a line in one of the jsonl files we receive as a response to our batch jobs
            response_booleans = line["response"]["body"]["choices"][0]["message"][
                "content"
            ].split("\n")
            response_booleans = ["True" in item for item in response_booleans]
            record = int(line["custom_id"])
            output_booleans.update({record: response_booleans})

    return output_booleans


def conform_blocks_to_response(blocks, output_booleans):
    # given the output_booleans produced by load_output_booleans and the blocks they were made as a response to,
    # trim the blocks such that only records marked as "True" in the output remain i.e. remove those where the output says "False"
    output_blocks = {}
    for key in blocks:
        if output_booleans.get(key, None) != None:
            for i in range(len(output_booleans[key])):
                if output_booleans[key][i]:
                    # if the value of an item in output_booleans[key] is True, add the item at the same index in blocks[key] to the output_blocks
                    output_blocks.update(
                        {key: output_blocks.get(key, list()) + [blocks[key][i]]}
                    )
        # if for any reason the output_booleans don't have a list for a certain record, just add an empty list to the output_blocks for that key
        output_blocks.update({key: output_blocks.get(key, list())})
    return output_blocks


def create_blocks_from_output_pairs(output_filepath):
    # given the path of a jsonl file produced as output to a batch job where individual name pairs are given in each request
    # create a dictionary that maps records to the records the LLM thinks they match with
    with open(output_filepath) as output_file:
        output_blocks = {}
        for line in output_file:
            line = json.loads(line)
            # the custom_id of each request is formatted as "record1#record2".
            # In the original blocks, record1 maps to a number of other records, among which record2 can be found.
            # We will use this custom_id to construct our output_blocks:
            record_pair = line["custom_id"].split("#")
            record = int(record_pair[0])
            possible_match = int(record_pair[1])
            print(
                f"Adding ({record}, {possible_match}) to blocks.     ",
                end="\r",
            )
            output_blocks.update({record: output_blocks.get(record, list())})
            response = line["response"]["body"]["choices"][0]["message"]["content"]
            if "True" in response:
                output_blocks.update({record: output_blocks[record] + [possible_match]})
        print("")
        return output_blocks


def create_blocks_from_output_pairs_with_confidence_score(
    output_filepath, threshold=0.8
):
    # given the path of a jsonl file produced as output to a batch job where individual name pairs are given in each request
    # and a threshold which the response (formatted as a floating point number) to each request must be above or equal to in order to be included,
    # create a dictionary that maps records to the records the LLM thinks they match with
    with open(output_filepath) as output_file:
        output_blocks = {}
        value_errors = 0
        match_scores = {}
        for line in output_file:
            line = json.loads(line)
            # the custom_id of each request is formatted as "record1#record2".
            # In the original blocks, record1 maps to a number of other records, among which record2 can be found.
            # We will use this custom_id to construct our output_blocks:
            record_pair = line["custom_id"].split("#")
            record = int(record_pair[0])
            possible_match = int(record_pair[1])
            print(
                f"Adding ({record}, {possible_match}) to blocks.     ",
                end="\r",
            )
            output_blocks.update({record: output_blocks.get(record, list())})
            match_scores.update({record: match_scores.get(record, list())})
            response = line["response"]["body"]["choices"][0]["message"]["content"]
            try:
                score = float(response)
                if float(response) >= threshold:
                    output_blocks.update(
                        {record: output_blocks[record] + [possible_match]}
                    )
                    match_scores.update({record: match_scores[record] + [score]})
            except ValueError:
                value_errors += 1
        for record in output_blocks:
            if len(match_scores[record]) > 0:
                max_score = max(match_scores[record])
                possible_matches = output_blocks[record].copy()
                for i, score in enumerate(match_scores[record]):
                    if score < max_score:
                        output_blocks[record].remove(possible_matches[i])
        print(f"\nSkipped {value_errors} responses due to incorrect format.")
        return output_blocks


def test_with_name_list():
    print("Loading response...")
    output_booleans = load_response_booleans(r"app\batchfile3test_output.jsonl")
    with open(r"app\blocks.json") as file:
        print("Retrieving blocks...")
        blocks = json.load(file)
        blocks = {int(k): list(v) for k, v in blocks.items()}
        print("Creating response blocks...")
        output_blocks = conform_blocks_to_response(blocks, output_booleans)
        matches = pd.read_csv(
            r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
            sep="\t",
            header=0,
        )
        precision = calculate_precision(output_blocks, matches)
        recall = calculate_recall_better(output_blocks, matches)
        f1 = 0
        if (precision + recall) != 0:
            f1 = 2 * (precision * recall) / (precision + recall)
        print(f"Precision: {precision}\nRecall: {recall}\nF1: {f1}")


def test_with_name_pairs():
    print("Creating response blocks...")
    output_blocks = create_blocks_from_output_pairs_with_confidence_score(
        r"experiments\partScores200ConfScore\partScores200ConfScoreoutput.jsonl"
    )
    matches = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
        sep="\t",
        header=0,
    )
    precision = calculate_precision(output_blocks, matches)
    recall = calculate_recall_better(output_blocks, matches)
    f1 = 0
    fB = 0
    if (precision + recall) != 0:
        f1 = 2 * (precision * recall) / (precision + recall)
        B = 5
        # NOTE replace B with something else if you want recall to be considered more or less important
        # fB = ((1 + B**2) * precision * recall) / (B**2 * precision) + recall
        fB = (1 + B**2) / ((B**2 * recall**-1) + precision**-1)
    print(f"Precision: {precision}\nRecall: {recall}\nF1: {f1}\nF{B}: {fB}")
    write_missed_matches(output_blocks, matches)


def write_missed_matches(
    output_blocks, matches, filepath=r"app\experiment_missed_matches.tsv"
):
    df2 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv", sep="\t", header=0
    )
    df1 = pd.read_csv(
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
        sep="\t",
        header=0,
    )
    missed_matches = find_missed_matches(output_blocks, matches)
    with open(filepath, "w") as file:
        file.write("df2_title\tdf2_name_parts\tdf1_title\tdf1_name_parts\n")
        for record in missed_matches:
            for missed_match in missed_matches[record]:
                df2_row = df2.iloc[record]
                df1_row = df1.iloc[missed_match]
                file.write(
                    f"{df2_row["title"]}\t{df2_row["name_parts"]}\t{df1_row["title"]}\t{df1_row["name_parts"]}\n"
                )


def update_blocks_with_list_names(
    output_filepath, blocks_filepath, leftover_blocks_filepath, blocks_df
):
    # given the path to an output file, the path to the blocks-file the input was created from, the dataset the blocks map to, and the path to the leftover blocks,
    # updates the blocks-file. # NOTE This function was made to work with prepare_batch_file_filter_lists.
    with open(output_filepath) as output_file, open(
        leftover_blocks_filepath
    ) as leftover_blocks_file:
        # load the the blocks we started with and load the leftover blocks which will be the base of our new blocks
        blocks_file = open(blocks_filepath)
        blocks = json.load(blocks_file)
        blocks_file.close()
        blocks = {int(k): set(v) for k, v in blocks.items()}
        new_blocks = json.load(leftover_blocks_file)
        new_blocks = {int(k): set(v) for k, v in new_blocks.items()}
        for line in output_file:
            line = json.loads(line)
            # get the response and the record we're trying to find a match for
            response = line["response"]["body"]["choices"][0]["message"]["content"]
            id = line["custom_id"]
            record = int(id.split("-")[0])
            # find out which record in the block was chosen and add it to the new blocks
            for possible_match in blocks[record]:
                # FIXME hardcoded to use the title right now. If the names get a different format, this must be changed!
                # NOTE we're checking if the name is in the response in case the response has other stuff in it too, like extra quotation marks or a prefix like "the most likely match is:"
                if blocks_df.iloc[possible_match]["title"] in response:
                    new_blocks[record].add(possible_match)
                    print(f"Found returned name for {id}")
                    break
        # we're done building the new blocks, now we overwrite the old blocks so we can make a new request out of them
        new_blocks = {k: list(v) for k, v in new_blocks.items()}
        with open(blocks_filepath, "w") as blocks_file:
            json.dump(new_blocks, blocks_file, ensure_ascii=False, indent=4)
        # if each key maps to exactly one record, then we're ready for pairwise comparisons and should tell the user
        if sum([len(x) for x in new_blocks.values()]) == len(new_blocks):
            print(
                "All records have one possible match. Ready for pairwise comparisons!"
            )
        else:
            print("Blocks updated and ready for next iteration!")


if __name__ == "__main__":
    test_with_name_pairs()
    # update_blocks_with_list_names(
    #    r"experiments\listsTestIteration3\listsTestIteration3output.jsonl",
    #    r"experiments\listsTestIteration3\filtered_blocks.json",
    #    r"experiments\listsTestIteration3\leftoverBlocks.jsonl",
    #    pd.read_csv(
    #        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
    #        sep="\t",
    #        header=0,
    #    ),
    # )
    pass
