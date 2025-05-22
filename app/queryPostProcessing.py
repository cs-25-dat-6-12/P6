import json
from textFiltering import load_data, filter_with_part_scores

def post_process(query_path, filter_function, block_size, output_path):
    df2, df1, matches = load_data(
        r"datasets\testset15-Zylbercweig-Laski\LASKI.tsv",
        r"datasets\testset15-Zylbercweig-Laski\Zylbercweig_roman.csv",
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
    
if __name__ == "__main__":
    post_process(r"app\test_queries_input.jsonl", filter_with_part_scores, 10, r"app\test_queries_result.jsonl")