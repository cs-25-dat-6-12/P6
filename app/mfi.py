import pandas as pd
from mlxtend.frequent_patterns import fpgrowth
import time
from typing import List, Dict, Set, Tuple
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def split_name_parts(dataset):
    id, title, primary, given, alt, sur, middle = [], [], [], [], [], [], []
    for i, row in dataset.iterrows():
        id.append(row["id"])
        title.append(row["title"])
        name_parts = row["name_parts"]
        name_parts = name_parts.replace("\"", "").replace("{", "").replace("}", "").replace(" ", "")
        name_part = name_parts.split(",")
        for part in name_part:
            name = part.split(":")
            if name[0] == "primary-name":
                primary.append(name[1])
            if name[0] == "given-name":
                given.append(name[1])
            if name[0] == "alternative-name":
                alt.append(name[1])
            if name[0] == "surname":
                sur.append(name[1])
            if name[0] == "middle-name":
                middle.append(name[1])
        if len(primary) != i+1:
            primary.append(None)
        if len(given) != i+1:
            given.append(None)
        if len(middle) != i+1:
            middle.append(None)
        if len(sur) != i+1:
            sur.append(None)
        if len(alt) != i+1:
            alt.append(None)
    return pd.DataFrame({
        "id": id,
        "title": title,
        "given-name": given,
        "alternative-name": alt,
        "primary-name": primary,
        "surname": sur,
        "middle-name": middle,
    })

def generate_blocks_mfi(df: pd.DataFrame, id_column: str, attribute_columns: List[str],
                       max_minsup_ratio: float = 0.05, p: float = 0.85, sparse_neigh: int = 3) -> Dict[str, List[str]]:
    """
    Generates blocks using maximal frequent itemsets (MFIs).

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        id_column (str): The column name containing record IDs.
        attribute_columns (List[str]): The column names to use for block generation.
        max_minsup_ratio (float): Ratio of the DataFrame size for the initial minimum support.
        p (float): Proportion threshold for the block size.
        sparse_neigh (int): Maximum number of blocks a record can belong to.

    Returns:
        Dict[str, List[str]]: A dictionary of blocks with keys as block identifiers and values as lists of record IDs.
    """
    start_time = time.time()
    max_minsup = int(len(df) * max_minsup_ratio)
    # Convert data to boolean format for FPGrowth
    encoded_data = pd.get_dummies(df[attribute_columns]).astype(bool)
    blocks = {}
    minsup = max_minsup
    covered_records = set()
    
    while minsup >= 2 and covered_records != set(df[id_column]):
        print(minsup-1)
        # Mine MFIs from uncovered records
        uncovered_mask = ~df[id_column].isin(covered_records)
        uncovered_data = encoded_data[uncovered_mask]
        mfis = fpgrowth(uncovered_data, min_support=minsup/len(df), use_colnames=True)
        for _, row in mfis.iterrows():
            block_items = set(row['itemsets'])
            if len(block_items) <= minsup * p:
                block_key = '-'.join(sorted(block_items))
                if block_key not in blocks:
                    blocks[block_key] = []
                
                # Find records that match this block
                for idx, record in df.iterrows():
                    if any(encoded_data.iloc[idx][col] for col in block_items):
                        record_id = str(record[id_column])
                        if sum(1 for block_ids in blocks.values() if record_id in block_ids) < sparse_neigh:
                            blocks[block_key].append(record_id)
                            covered_records.add(record_id)
        
        minsup -= 1
    
    logging.info(f"Block generation completed in {time.time() - start_time:.2f} seconds.")
    print(blocks)
    print("\n")
    return blocks

def generate_clean_clean_blocks_mfi(df1: pd.DataFrame, df2: pd.DataFrame, index_path1: str, index_path2: str,
                                     id_column: str, attribute_columns_df1: List[str], attribute_columns_df2: List[str],
                                     max_minsup: int = 4, similarity_threshold: float = 0.85) -> Dict[str, List[Tuple[List[str], List[str]]]]:
    try:
        # Generate blocks for df1 and df2 using respective attribute columns
        print("start")
        blocks_df1 = generate_blocks_mfi(df1, id_column, attribute_columns_df1, max_minsup, similarity_threshold)
        print("block 1 done")
        blocks_df2 = generate_blocks_mfi(df2, id_column, attribute_columns_df2, max_minsup, similarity_threshold)
        print("block 2 done")

        matched_blocks = []

        # Match blocks between df1 and df2
        for block_key in blocks_df1:
            if block_key in blocks_df2:
                matched_blocks.append((blocks_df1[block_key], blocks_df2[block_key]))

        logging.info(f"Number of matched blocks: {len(matched_blocks)}")
        
        # Return matched blocks wrapped in a dictionary with the "blocks" key
        return {"blocks": matched_blocks, "error": []}  # Returning in the expected structure

    except Exception as e:
        logging.error(f"Error in MFI blocking: {str(e)}")
        # Return the error in the expected dictionary format
        return {"blocks": [], "error": [str(e)]}
if __name__ == "__main__":
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    zylbercweig = split_name_parts(zylbercweig)
    laski = split_name_parts(laski)
    blocks = generate_clean_clean_blocks_mfi(zylbercweig, laski, "index1", "index2", "id", ["title", "given-name", "primary-name", "alternative-name", "surname", "middle-name"], ["title", "given-name", "primary-name", "alternative-name", "surname", "middle-name"], 0.002, 0.5)
    for key in blocks:
        print(key)
