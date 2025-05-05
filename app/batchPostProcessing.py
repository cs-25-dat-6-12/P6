import json

import pandas as pd
from textFiltering import calculate_recall_better, calculate_precision


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
            output_blocks.update({record: output_blocks.get(record, list())})
            response = line["response"]["body"]["choices"][0]["message"]["content"]
            if "True" in response:
                output_blocks.update({record: output_blocks[record] + [possible_match]})
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
    output_blocks = create_blocks_from_output_pairs(r"app\partScores400output.jsonl")
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


if __name__ == "__main__":
    test_with_name_pairs()
