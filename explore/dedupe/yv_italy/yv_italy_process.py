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
from unidecode import unidecode

# Preprocessing function to clean and normalize column values
def preProcess(column):
    column = unidecode(column)  # Remove any accent marks
    column = re.sub("  +", " ", column)  # Replace multiple spaces with a single space
    column = re.sub("\n", " ", column)  # Remove newline characters
    column = column.strip().strip('"').strip("'").lower().strip()  # Strip extra spaces and normalize case
    
    # Handle missing data
    if not column:
        return None
    
    return column

# Read and preprocess data from CSV file, ignoring lat and lon columns
def readData(filename):
    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = {}
            for k, v in row.items():
                if k not in ["lat", "lon"]:  # Ignore lat and lon columns
                    clean_row[k] = preProcess(v)
            row_id = int(row["Id"])
            data_d[row_id] = clean_row
    
    return data_d

# Main execution
if __name__ == "__main__":

    optp = optparse.OptionParser()
    optp.add_option("-v", "--verbose", dest="verbose", action="count", help="Increase verbosity")
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.basicConfig(level=log_level)

    logging.getLogger('dedupe').setLevel(logging.DEBUG)

    input_file = "yv_italy.csv"
    output_file = "yv_italy_output.csv"
    settings_file = "yv_italy_learned_settings"
    training_file = "yv_italy_training.json"

    print("Importing data...")
    data_d = readData(input_file)

    # Debugging: Add print statements to inspect data_d
    print(type(data_d))

    if os.path.exists(settings_file):
        print(f"Reading from {settings_file}")
        with open(settings_file, "rb") as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        fields = [
            dedupe.variables.String("title"),
            dedupe.variables.Categorical("gender", ["male", "female"]),
            dedupe.variables.String("birth_date", has_missing=True),
            dedupe.variables.String("title_source", has_missing=True),
            dedupe.variables.Text("associatedPlaces", has_missing=True),
            dedupe.variables.String("birth_place", has_missing=True),
            dedupe.variables.String("death_place", has_missing=True),
            dedupe.variables.Text("name_parts", has_missing=True)
        ]

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
            deduper.prepare_training(data_d)  # Debugging: Ensure data_d is structured correctly
        print("Finished deduper.prepare_training()")

        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
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

    with open(output_file, "w") as f_output, open(input_file) as f_input:
        reader = csv.DictReader(f_input)
        fieldnames = ["Cluster ID", "confidence_score"] + reader.fieldnames
        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            row_id = int(row["Id"])
            row.update(cluster_membership[row_id])
            writer.writerow(row)