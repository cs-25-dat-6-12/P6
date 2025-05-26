import json
import os
from textFiltering import (
    load_data,
    filter_with_part_scores,
    filter_with_normalized_scores_revised,
)
from batchPostProcessing import test_with_name_pairs_new


def post_process(query_path, filter_function, block_size, output_path):
    df2, df1, _ = load_data(
        r"datasets\phonetic\LASKI_phonetic.csv",
        r"datasets\phonetic\Zylbercweig_phonetic.csv",
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
    )

    with open(r"app\blocks.json") as file:
        # load the blocks created in the blocking phase and make the lists into sets again
        blocks = json.load(file)
        blocks = {int(k): set(v) for k, v in blocks.items()}
        filtered_blocks = filter_function(blocks, df2, df1, block_size)
        with open(query_path) as file:
            output = ""
            for line in file:
                record_pair = json.loads(line)["custom_id"].split("#")
                record = int(record_pair[0])
                possible_match = int(record_pair[1])
                if possible_match in filtered_blocks[record]:
                    output += line
        with open(output_path, "w") as file:
            file.write(output)


def post_process_phonetic(query_path, block_size):
    with open(f"app/phonetic_blocks/{block_size}.json") as file:
        filtered_blocks = json.load(file)
        filtered_blocks = {int(k): set(v) for k, v in filtered_blocks.items()}
    with open(query_path) as file:
        # output = ""
        output = []
        for line in file:
            record_pair = json.loads(line)["custom_id"].split("#")
            record = int(record_pair[0])
            possible_match = int(record_pair[1])
            if possible_match in filtered_blocks[record]:
                # output += line
                output.append(line)
    # with open(r"app\temp.jsonl", "w") as file:
    # file.write(output)
    return output


def create_block_files(filter_function):
    df2, df1, _ = load_data(
        r"datasets\phonetic\LASKI_phonetic.csv",
        r"datasets\phonetic\Zylbercweig_phonetic.csv",
        r"datasets\testset15-Zylbercweig-Laski\transliterated_em.csv",
    )
    with open(r"app\blocks.json") as file:
        blocks = json.load(file)
        blocks = {int(k): set(v) for k, v in blocks.items()}
        for i in range(1, 21):
            if not i == 20:
                continue
            filtered_blocks = filter_function(blocks, df2, df1, i * 10)
            path = f"app/phonetic_blocks/{i*10}.json"
            with open(path, "w", encoding="utf-8") as file:
                filtered_blocks_new = {}
                for k, v in filtered_blocks.items():
                    array = []
                    for item in v:
                        array.append(item)
                    filtered_blocks_new.update({k: array})
                json.dump(filtered_blocks_new, file, indent=4)


def create_query_files(query_path, range_object=range(10, 200 + 1, 10)):
    name = query_path.replace(".jsonl", "").replace("app/", "")
    for i in range_object:
        output_string = ""
        print(f"Starting block size = {i}")
        array = post_process_phonetic(query_path, i)
        output_string += f"Block size: {i}\n"
        output_string += test_with_name_pairs_new(array) + "\n\n"
        try:
            os.makedirs(f"app/stats_" + name[: -len(os.path.basename(name))])
        except FileExistsError:
            pass
        with open(f"app/stats_" + name + ".txt", "a") as file:
            file.write(output_string)


if __name__ == "__main__":
    ##create_block_files(filter_with_normalized_scores_revised)
    # post_process(r"app\test_queries_input.jsonl", filter_with_part_scores, 10, r"app\test_queries_result.jsonl")
    create_query_files(
        r"experiments\PaperExperiments\ZylbercweigLaski\PhoneticExtras\PhoneticTitle\PhoneticTitleoutput.jsonl",
        range(50, 200 + 1, 50),
    )
