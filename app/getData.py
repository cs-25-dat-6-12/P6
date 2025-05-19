import csv
import re
from unidecode import unidecode
import pandas as pd

string_feature_set = set()
full_feature_list = []
rows = []

def preProcess(column):
    if isinstance(column, (int, float)):
        return str(
            column
        )  # No need for unidecode on numeric data(unidecode doesn't work on numeric data)
    elif column is None:
        return None

    column = unidecode(column)
    column = re.sub("  +", " ", column)
    column = re.sub("\n", " ", column)
    column = column.strip().strip('"').strip("'").lower().strip()

    # Handle missing data
    return column if column else None


# Read and preprocess data from CSV file, ignoring lat and lon columns
def readData(filenames):
    data_d = {}
    
    # If filenames is a DataFrame, just use it directly
    if isinstance(filenames, pd.DataFrame):
        rows = filenames.to_dict(orient='records')  # Convert DataFrame rows to list of dictionaries
    else:
        rows = []
        file_number = -1
        for file in filenames:
            file_number += 1
            with open(file, encoding="utf8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row.update({"data_source": file_number})
                    rows.append(row)
    
    # Rest of the code remains the same...
    print("Finding distinct name parts")
    bad_rows = []
    for row in rows:
        for feature in row:
            if feature not in full_feature_list:
                full_feature_list.append(feature)
        try:
            for feature in eval(
                row["name_parts"]
                .replace('""""', '""#""')
                .replace('""', '"')
                .replace('"#"', '""')
            ):
                string_feature_set.add(feature)
        except SyntaxError:
            bad_rows.append(row)
    print(f"Name parts found: {string_feature_set}")

    # Remove bad rows
    for row in bad_rows:
        print(f"Row {row['id']} has badly formatted name parts. Discarding it.")
        rows.remove(row)

    # Assign new IDs and clean rows
    next_id = 0
    for row in rows:
        row.update({"id": next_id})
        next_id = next_id + 1

    print("Cleaning rows")
    for row in rows:
        clean_row = {
            k: preProcess(v)
            for k, v in row.items()
            if k not in ["", "start", "id", "geo_id", "geowkt", "geo_source", "title_source", "description", "lat", "lon"] #, "title", "name_parts"
        }

        clean_row["title"] = preProcess(row.get("title", ""))

        if row.get("name_parts") is not None:
            name_parts = eval(
                row["name_parts"]
                .replace('""""', '""#""')
                .replace('""', '"')
                .replace('"#"', '""')
            )
            for feature in string_feature_set:
                clean_row[feature] = name_parts.get(feature)

        if row.get("birth_date"):
            clean_row["birth_date"] = row["birth_date"].replace("-", "/")

        if row.get("death_date"):
            clean_row["death_date"] = row["death_date"].replace("-", "/")

        row_id = int(row["id"])
        data_d[row_id] = clean_row
    
    df = pd.DataFrame.from_dict(data_d, orient="index")
    df["id"] = df.index

    return df

def langData(data,lang):
    data = data[data['lang'] == lang]
    return data

def matchFile(data, lang1, lang2):
    
    return data

if __name__ == "__main__":

    wikiData = pd.read_csv("datasets\wikiData\wikiMedLink.csv")
    wikiData_yi = langData(wikiData,"yi")
    wikiData_en = langData(wikiData,"en")
    print(wikiData_en)