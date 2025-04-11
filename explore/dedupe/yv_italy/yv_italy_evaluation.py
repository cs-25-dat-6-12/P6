import collections
import csv
import itertools


def evaluateDuplicates(found_dupes, true_dupes):
    true_positives = found_dupes.intersection(true_dupes)
    false_positives = found_dupes.difference(true_dupes)

    print("Found duplicate pairs:")
    print(len(found_dupes))

    print("Precision:")
    print(1 - len(false_positives) / float(len(found_dupes)))

    print("Recall:")
    print(len(true_positives) / float(len(true_dupes)))


def dupePairs(filename, rowname, judgement_column=None):
    dupe_d = collections.defaultdict(list)

    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=",", quotechar='"')
        for row in reader:
            # For manual clusters, we use the judgement column to determine if a pair is a duplicate
            if judgement_column:
                if row[judgement_column] == 'TRUE':  # If judgement is 'TRUE', it's a duplicate
                    dupe_d[row[rowname]].append(row["id_1"])
                    dupe_d[row[rowname]].append(row["id_2"])
            else:
                # For dedupe output, we use Cluster ID to identify duplicates
                dupe_d[row[rowname]].append(row["Id"])

    # Remove any extraneous or incorrect data points (e.g., 'x' if exists)
    if "x" in dupe_d:
        del dupe_d["x"]

    dupe_s = set()
    for unique_id, cluster in dupe_d.items():
        if len(cluster) > 1:
            # Generate all possible pairs from the cluster
            for pair in itertools.combinations(cluster, 2):
                dupe_s.add(frozenset(pair))

    return dupe_s


manual_clusters = "em.csv"
dedupe_clusters = "yv_italy_output.csv"

# For manual clusters, use the 'judgement' column to find the true duplicates
true_dupes = dupePairs(manual_clusters, "id_1", judgement_column="judgement")

# For dedupe output, use the 'Cluster ID' to find duplicates
test_dupes = dupePairs(dedupe_clusters, "Cluster ID")

evaluateDuplicates(test_dupes, true_dupes)
