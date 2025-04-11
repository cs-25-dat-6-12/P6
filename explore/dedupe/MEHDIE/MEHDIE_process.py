#!/usr/bin/python
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.
"""

import csv
import logging
import optparse
import os
import re
import cProfile
import pstats
import io

import dedupe
import dedupe.variables
import datetimetype
from unidecode import unidecode

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
    # add the rows from both files to a list to join them. Keep track of where the rows came from
    file_number = -1
    for file in filenames:
        file_number += 1
        with open(file, encoding="utf8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row.update({"data_source": file_number})
                rows.append(row)

    # find out all features that exist and which features are in the name_parts dicts
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

    # remove any rows that threw a SyntaxError when their name parts were evaluated
    for row in bad_rows:
        print(f"Row {row["id"]} has badly formatted name parts. Discarding it.")
        rows.remove(row)

    # assigning new IDs to every row to ensure the data is cohesive (and to get around the wierd ID schemes of both datasets)
    next_id = 0
    for row in rows:
        row.update({"id": next_id})
        next_id = next_id + 1

    print("Cleaning rows")
    for row in rows:
        clean_row = {
            k: preProcess(v)
            for k, v in row.items()
            if k not in ["lat", "lon", "title", "name_parts"]
        }

        if row.get("name_parts") != None:
            name_parts = eval(
                row["name_parts"]
                .replace('""""', '""#""')
                .replace('""', '"')
                .replace('"#"', '""')
            )
            for feature in string_feature_set:
                clean_row.update({feature: name_parts.get(feature)})
        if row.get("birth_date"):
            clean_row.update({"birth_date": row.get("birth_date").replace("-", "/")})

        if row.get("death_date"):
            clean_row.update({"death_date": row.get("death_date").replace("-", "/")})

        row_id = int(row["id"])
        data_d[row_id] = clean_row

    return data_d


def same_or_not_comparator(field_1, field_2):
    if field_1 and field_2:
        if field_1 == field_2:
            return 0
        else:
            return 1


def different_or_not_comparator(field_1, field_2):
    if field_1 and field_2:
        if field_1 != field_2:
            return 0
        else:
            return 10000000


# Main execution
if __name__ == "__main__":

    optp = optparse.OptionParser()
    optp.add_option(
        "-v", "--verbose", dest="verbose", action="count", help="Increase verbosity"
    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.basicConfig(level=log_level)

    logging.getLogger("dedupe").setLevel(logging.DEBUG)

    # setup

    output_file = "MEHDIE_output.csv"
    settings_file = "MEHDIE_learned_settings"
    training_file = "MEHDIE_training.json"

    LASKI_file = os.path.join("data", "LASKI.csv")
    Zylbercweig_file = os.path.join("data", "Zylbercweig_roman.csv")

    print("Importing data...")
    input_files = {LASKI_file, Zylbercweig_file}
    data_d = readData(input_files)

    # Debugging: Add print statements to inspect data_d
    print(type(data_d))

    if os.path.exists(settings_file):
        print(f"Reading from {settings_file}")
        with open(settings_file, "rb") as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # Set variables for comparison
        fields = [
            # dedupe.variables.Custom("gender", same_or_not_comparator, has_missing=True),
            # datetimetype.DateTime("birth_date", yearfirst=True, has_missing=True),
            # datetimetype.DateTime("death_date", yearfirst=True, has_missing=True),
            # dedupe.variables.String("birth_place", has_missing=True),
            # dedupe.variables.String("death_place", has_missing=True),
            # dedupe.variables.String("associatedPlaces", has_missing=True),
            dedupe.variables.Custom("data_source", different_or_not_comparator),
        ]
        # Add name parts for comparison
        for feature in string_feature_set:
            fields.append(dedupe.variables.String(feature, has_missing=True))

        deduper = dedupe.Dedupe(fields)

        pr = cProfile.Profile()
        pr.enable()

        print("Starting deduper.prepare_training()")
        if os.path.exists(training_file):
            print(f"Reading labeled examples from {training_file}")
            with open(training_file, "rb") as f:
                # Debugging: Check if candidate_predicates is being passed correctly
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(
                data_d
            )  # Debugging: Ensure data_d is structured correctly
        print("Finished deduper.prepare_training()")

        pr.disable()
        s = io.StringIO()
        sortby = "cumulative"
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())

        print("Starting active labeling...")
        dedupe.console_label(deduper)
        deduper.train()

        with open(training_file, "w") as tf:
            deduper.write_training(tf)

        with open(settings_file, "wb") as sf:
            deduper.write_settings(sf)

    print("Clustering...")
    clustered_dupes = deduper.partition(data_d, 0.5)

    print(f"# duplicate sets: {len(clustered_dupes)}")

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score,
            }

    with open(output_file, "w", encoding="utf-8") as f_output:
        fieldnames = ["Cluster ID", "confidence_score"] + full_feature_list
        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row_id = int(row["id"])
            row.update(cluster_membership[row_id])
            writer.writerow(row)
