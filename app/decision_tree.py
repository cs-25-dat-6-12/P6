import pandas as pd
from sklearn.datasets import load_iris
from sklearn import tree

class Potential_matches():
    ids = []
    distances = [[], [], [], [], [], [], [], [], [], [], [], []]
    verdict = []
    parts = ["primary-name", "given-name", "alternative-name", "surname", "patronymic", "middle-name", "acronym", "honorific", "appellation", "nisba", "professional-name", "salutation"]

def example():
    iris = load_iris()
    print(iris.data)
    print(iris.target)
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(iris.data, iris.target)
    print(iris['feature_names'])
    print(tree.export_text(clf, feature_names=iris['feature_names']))

def levenshtein_distance(str1, str2):
    m = len(str1)
    n = len(str2)
 
    matrix = [[0 for _ in range(n + 1)] for _ in range(m + 1)]
 
    for i in range(m + 1):
        matrix[i][0] = i
    for j in range(n + 1):
        matrix[0][j] = j
 
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if str1[i - 1] == str2[j - 1]:
                matrix[i][j] = min(matrix[i - 1][j - 1], 1 + matrix[i][j - 1], 1 + matrix[i - 1][j])
            else:
                matrix[i][j] = 1 + min(matrix[i][j - 1], matrix[i - 1][j], matrix[i - 1][j - 1])
    return matrix[m][n]

def find_name_parts():
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    parts1 = ["primary-name"]
    parts2 = ["primary-name"]
    for _, row in zylbercweig.iterrows():
        name_parts = row["name_parts"]
        name_parts = name_parts.replace("\"", "").replace("{", "").replace("}", "").replace(" ", "")
        name_part = name_parts.split(",")
        for part in name_part:
            i = 0
            for name in parts1:
                if part.split(":")[0] != name:
                    i = i+1
            if i == len(parts1):
                parts1.append(part.split(":")[0])

    for _, row in laski.iterrows():
        name_parts = row["name_parts"]
        name_parts = name_parts.replace("\"", "").replace("{", "").replace("}", "").replace("alternative-name.1", "alternative-name")
        name_part = name_parts.split(",")
        for part in name_part:
            i = 0
            for name in parts2:
                if part.split(":")[0] != name:
                    i = i+1
            if i == len(parts2):
                parts2.append(part.split(":")[0])
    
    print(parts1)
    print(parts2)

def find_distance(output_path):
    # TODO Update correct matches
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    table = Potential_matches()
    for _, row in zylbercweig.iterrows():
        #if _ > 10:
            #continue
        if _ % 50 == 0 or _ == len(zylbercweig):
            print(str(_) + " out of " + str(len(zylbercweig)))
        bool_table = zylbercweig.isna()
        if not bool_table["geo_source"][_]:
            source = row["geo_source"].replace("laski:", "")
        else:
            source = ""
        for _, row2 in laski.iterrows():
            id1 = row["id"]
            id2 = row2["id"]
            table.ids.append([id1, id2])
            name_parts_zylbercweig = row["name_parts"].replace("\"", "").replace("{", "").replace("}", "").replace(" ", "")
            name_part_zylbercweig = name_parts_zylbercweig.split(",")
            name_parts_laski = row2["name_parts"].replace("\"", "").replace("{", "").replace("}", "").replace(" ", "")
            name_part_laski = name_parts_laski.split(",")
            distances_found = []
            if source == id2:
                table.verdict.append(1)
            else:
                table.verdict.append(-1)
            for name_zylbercweig in name_part_zylbercweig:
                name_zylbercweig = name_zylbercweig.split(":")
                for name_laski in name_part_laski:
                    name_laski = name_laski.split(":")
                    if name_zylbercweig[0] == name_laski[0] and name_zylbercweig[0] in table.parts:
                        distance = levenshtein_distance(name_zylbercweig[1], name_laski[1])
                        index = find_name_part_index(name_zylbercweig[0])
                        table.distances[index].append(distance)
                        distances_found.append(index)
            for i, _ in enumerate(Potential_matches.parts):
                if i not in distances_found:
                    table.distances[i].append(-1)
    output = pd.DataFrame({
        "ids": table.ids,
        "primary-name-dist": table.distances[0],
        "given-name-dist": table.distances[1],
        "alt-name-dist": table.distances[2],
        "surname-dist": table.distances[3],
        "patronymic-dist": table.distances[4],
        "middle-name-dist": table.distances[5],
        "acronym-dist": table.distances[6],
        "honorific-dist": table.distances[7],
        "appelation-dist": table.distances[8],
        "nisba-dist": table.distances[9],
        "professional-name-dist": table.distances[10],
        "salutation-dist": table.distances[11],
        "result": table.verdict
    })
    print("printing result")
    output.to_csv(output_path, sep="\t")


def find_name_part_index(name_part):
    for i, part in enumerate(Potential_matches.parts):
        if name_part == part:
            return i
        
def make_decision_tree(output_path):
    print("reading inputfile")
    dataset = pd.read_csv("datasets/testset15-Zylbercweig-Laski/name_distances.csv", sep="\t")
    print("inputfile read")
    distances, results = [], []
    for _, row in dataset.iterrows():
        if _ % 100000 == 0:
            print(str(_) + " out of " + str(len(dataset)))
        distancearray = []
        distancearray.append(row["primary-name-dist"])
        distancearray.append(row["given-name-dist"])
        distancearray.append(row["alt-name-dist"])
        distancearray.append(row["surname-dist"])
        distancearray.append(row["patronymic-dist"])
        distancearray.append(row["middle-name-dist"])
        distancearray.append(row["acronym-dist"])
        distancearray.append(row["honorific-dist"])
        distancearray.append(row["appelation-dist"])
        distancearray.append(row["nisba-dist"])
        distancearray.append(row["professional-name-dist"])
        distancearray.append(row["salutation-dist"])
        distances.append(distancearray)
        results.append(row["result"])
    print("training tree")
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(distances, results)
    f = open(output_path, "w")
    f.write(tree.export_text(clf, feature_names=Potential_matches.parts, max_depth=30, show_weights=True))
    f.close()
    
if __name__ == "__main__":
    find_distance("datasets/testset15-Zylbercweig-Laski/name_distances.csv")
    make_decision_tree("datasets/testset15-Zylbercweig-Laski/tree")