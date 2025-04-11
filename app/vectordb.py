import chromadb
import pandas as pd
from chromadb.utils import embedding_functions

class Embedding_function_names():
    names = ["all-MiniLM-L6-v2", "all-MiniLM-L12-v2", "all-distilroberta-v1", "all-mpnet-base-v2"]

def get_db(name):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    collection = db_client.get_collection(name=name)
    return collection

def create_db_zylbercweig_laski(name="Zylbercweig-LASKI", embedding_model_name = "all-MiniLM-L6-v2", pre_process=False, print_progress=False):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)
    collection = db_client.create_collection(name=name, embedding_function=embedding_function)
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    laski_bool = laski.isna()
    for _, row in zylbercweig.iterrows():
        if print_progress and _%100 == 0:
            print("Zylbercweig", _)
        name = row["title"]
        if pre_process:
            name_parts = []
            split1 = name.split("(")
            if len(split1) != 1 and split1[0] != "":
                name_parts.append(split1[0])
                split2 = split1[1].split(")")
                if len(split2) != 1 and split2[1] != "":
                    name_parts.append(split2[0])
                    name_parts.append(split2[1])
                if len(name_parts) == 3:
                    print("before:", name)
                    name = name_parts[0] + name_parts[2][1:len(name_parts[2])]
                    print("after", name)
                else:
                    name = name.replace("(", "").replace(")", "")
            else:
                name = name.replace("(", "").replace(")", "")
        id = row["id"]
        add_name_to_db(collection, name, "Zylbercweig", str(id))
    for i, row in laski.iterrows():
        if laski_bool["id"][i]:
            continue
        if print_progress and i%100 == 0:
            print("LASKI", i)
        name = row["title"]
        id = row["id"]
        add_name_to_db(collection, name, "LASKI", id)
    return collection

def add_name_to_db(collection, name, source, id):
    collection.upsert(documents=name, ids=id, metadatas=[{"source": source}])
    
def add_embedding_to_db(collection, embedding, name, source, id):
    collection.upsert(embeddings=embedding ,documents=name, ids=id, metadatas=[{"source": source}])

def query_db_by_name(collection, name, source="n/a", results_amount=3, include=["documents", "distances"]):
    if not source == "n/a":
        return collection.query(query_texts=name, n_results=results_amount, where={"source": source}, include=include)
    else:
        return collection.query(query_texts=name, n_results=results_amount, include=include)

def query_db_by_embedding(collection, embedding, source="n/a", results_amount=3, include=["documents", "distances"]):
    if not source == "n/a":
        return collection.query(query_embeddings=embedding, n_results=results_amount, where={"source": source}, include=include)
    else:
        return collection.query(query_embeddings=embedding, n_results=results_amount, include=include)
    
def calc_db_recall(collection, matches_path="datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv", candidates=5, pre_process=False, print_progress=False):
    dataset = pd.read_csv(matches_path, sep="\t")
    match_total, match_laski, match_zylbercweig = False, False, False
    comparisons, matches_total, matches_laski, matches_zylbercweig = 0, 0, 0, 0
    for _, row in dataset.iterrows():
        if print_progress and _%50 == 0:
            print(_, "out of:", len(dataset))
        zylbercweig = row["transliterated_name"]
        if pre_process:
            name_parts = []
            split1 = zylbercweig.split("(")
            if len(split1) != 1 and split1[0] != "":
                name_parts.append(split1[0])
                split2 = split1[1].split(")")
                if len(split2) != 1 and split2[1] != "":
                    name_parts.append(split2[0])
                    name_parts.append(split2[1])
                if len(name_parts) == 3:
                    zylbercweig = name_parts[0] + name_parts[2][1:len(name_parts[2])]
                else:
                    zylbercweig = name.replace("(", "").replace(")", "")
            else:
                zylbercweig = zylbercweig.replace("(", "").replace(")", "")
        laski = row["title"]
        query_zylbercweig = query_db_by_name(collection, zylbercweig, "LASKI", candidates)["documents"][0]
        query_laski = query_db_by_name(collection, laski, "Zylbercweig", candidates)["documents"][0]
        match_total = False
        match_laski = False
        match_zylbercweig = False
        for name in query_zylbercweig:
            if name == laski:
                match_zylbercweig = True
                match_total = True
        for name in query_laski:
            if name == zylbercweig:
                match_laski = True
                match_total = True
        if match_total == True:
            matches_total += 1
            if match_zylbercweig == True:
                matches_zylbercweig += 1
            if match_laski == True:
                matches_laski += 1
        comparisons += 1
    recall_laski = matches_laski/comparisons
    recall_zylbercweig = matches_zylbercweig/comparisons
    recall_total = matches_total/comparisons
    log_string = f'modelname: {collection.name}\n' + f'Total matches checked: {comparisons} with {candidates} candidates per match\n' + f'Recall LASKI: {recall_laski}\n' + f'Recall zylbercweig: {recall_zylbercweig}\n' + f'Recall total: {recall_total}'
    print(log_string)
    f = open("db/vectordb.chroma/logfile.txt", "a")
    f.write(log_string + "\n\n")
    f.close()

if __name__ == "__main__":
    for name in Embedding_function_names.names:
        model_name = "Zylbercweig-LASKI" + name
        collection = get_db(model_name)
        calc_db_recall(collection, print_progress=True)
        model_name = "Zylbercweig-LASKI" + name + "first/sur"
        collection = get_db(model_name)
        calc_db_recall(collection, print_progress=True)