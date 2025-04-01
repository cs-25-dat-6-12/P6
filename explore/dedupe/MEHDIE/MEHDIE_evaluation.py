import collections
import csv
import itertools
import pandas as pd


def evaluateDuplicates(stats):

    print(stats)

    print("Precision:")
    print(1 - stats["false_positives"] / float(stats["total_positives"]))

    print("Recall:")
    print(stats["true_positives"] / float(stats["total_true"]))


def getOutputStats():
    # the files to look at
    manual_clusters = "Transliterated_em.csv"
    dedupe_clusters = "MEHDIE_output.csv"
    with open(manual_clusters, encoding="utf8") as mc:
        true_positives = 0  # how many times did we pair two people correctly
        total_true = 0  # how many actual matching pairs are there in total
        total_positives = 0  # how many times did we pair two people in total

        mc_reader = csv.DictReader(mc)
        for row in mc_reader:
            total_true += 1
            print(f"Pair {total_true}:")
            name_parts_roman = row.get("name_parts_roman")
            print(name_parts_roman)
            name_parts_LASKI = row.get("name_parts_LASKI")
            print(name_parts_LASKI)
            roman_cluster = -1
            LASKI_cluster = -1
            with open(dedupe_clusters, encoding="utf8") as dc:
                dc_reader = csv.DictReader(dc)
                for row in dc_reader:
                    cluster_id = int(row.get("Cluster ID"))
                    # find true positives
                    if row.get("name_parts") == name_parts_roman:
                        roman_cluster = cluster_id
                        print(f"Found roman_custer: {cluster_id}")
                    if row.get("name_parts") == name_parts_LASKI:
                        LASKI_cluster = cluster_id
                        print(f"Found LASKI_cluster: {cluster_id}")
                    if (
                        roman_cluster > -1
                        and LASKI_cluster > -1
                        and roman_cluster == LASKI_cluster
                    ):
                        # two people are in the same cluster and those two people are a confirmed match
                        true_positives += 1
                        continue
        # find total number of positives
        # reset reader
        clusters = {}
        with open(dedupe_clusters, encoding="utf8") as dc:
            dc_reader = csv.DictReader(dc)
            for row in dc_reader:
                cluster_id = row.get("Cluster ID")
                clusters.update({cluster_id: clusters.get(cluster_id, 0) + 1})

        for cluster in clusters:
            n = clusters.get(cluster)
            total_positives += n * (n - 1) / 2

        # find false positives
        # how many times did we pair two people incorrectly
        false_positives = total_positives - true_positives

        return {
            "true_positives": true_positives,
            "false_positives": false_positives,
            "total_positives": total_positives,
            "total_true": total_true,
        }


evaluateDuplicates(getOutputStats())
