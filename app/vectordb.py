import chromadb
import pandas as pd
from chromadb.utils import embedding_functions

class Embedding_function_names():
    names = ["all-MiniLM-L6-v2", "all-MiniLM-L12-v2", "all-distilroberta-v1", "all-mpnet-base-v2", "all-roberta-large-v1", "sentence-t5-xxl"]

def get_db(name):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    collection = db_client.get_collection(name=name)
    return collection

def create_db_zylbercweig_laski(name="Zylbercweig-LASKI", embedding_model_name = "all-MiniLM-L6-v2", pre_process=False, print_progress=False):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)
    collection = db_client.get_or_create_collection(name=name, embedding_function=embedding_function)
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
                    name = name_parts[0] + name_parts[2][1:len(name_parts[2])]
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
    
def calc_db_recall(collection, matches_path="datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv", candidates=5, pre_process=False, print_progress=False, logging=False):
    print(f'Starting recall calculations on the {collection.name} model, with {candidates} canditates per match and first/surname == {pre_process}')
    dataset = pd.read_csv(matches_path, sep="\t")
    match_total, match_laski, match_zylbercweig = False, False, False
    comparisons, matches_total, matches_laski, matches_zylbercweig = 0, 0, 0, 0
    id_table = []
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
        id = row["key"]
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
            id_table.append(id)
            if match_zylbercweig == True:
                matches_zylbercweig += 1
            if match_laski == True:
                matches_laski += 1
        comparisons += 1
    recall_laski = matches_laski/comparisons
    recall_zylbercweig = matches_zylbercweig/comparisons
    recall_total = matches_total/comparisons
    log_string = f'modelname: {collection.name}\n' + f'Total matches checked: {comparisons} with {candidates} candidates per match\n' + f'Recall LASKI: {recall_laski}\n' + f'Recall zylbercweig: {recall_zylbercweig}\n' + f'Recall total: {recall_total}'
    if logging:
        f = open("db/vectordb.chroma/logfile_recall.txt", "a")
        f.write(log_string + "\n\n")
        f.close()
    else:
        print(log_string)
    return id_table

def find_model_similarity(model1, model2, candidates=5 ,model1_pre_process=False, model2_pre_process=False, print_progress=False, logging_similarity=False, logging_recall=False):
    # Calculates the Jaccard similarity between 2 models
    id_table1 = calc_db_recall(model1, candidates=candidates, pre_process=model1_pre_process, print_progress=print_progress, logging=logging_recall)
    id_table2 = calc_db_recall(model2, candidates=candidates, pre_process=model2_pre_process, print_progress=print_progress, logging=logging_recall)
    matches = 0
    for id in id_table1:
        if id in id_table2:
            matches += 1
    total_ids = len(id_table1) + len(id_table2) - matches
    similarity = matches/total_ids
    log_string = f'Jaccard similarity between {model1.name} and {model2.name}: {matches}/{total_ids}={similarity}'
    if logging_similarity:
        f = open("db/vectordb.chroma/logfile_jaccard_similarity.txt", "a")
        f.write(log_string + "\n\n")
        f.close()
    else:
        print(log_string)

if __name__ == "__main__":
    model_name1 = "Zylbercweig-LASKI" + "sentence-t5-xxl"
    model_name2 = "Zylbercweig-LASKI" + "sentence-t5-xxl" + "first-sur"
    collection1 = create_db_zylbercweig_laski(model_name1, embedding_model_name="sentence-t5-xxl", print_progress=True)
    collection2 = create_db_zylbercweig_laski(model_name2, embedding_model_name="sentence-t5-xxl", pre_process=True ,print_progress=True)
    find_model_similarity(collection1, collection2, 5, False, True, True, True, True)
    #for name in Embedding_function_names.names:
        #model_name = "Zylbercweig-LASKI" + name
        #collection1 = get_db(model_name)
        #model_name = "Zylbercweig-LASKI" + name + "first-sur"
        #collection2 = get_db(model_name)
        #find_model_similarity(collection1, collection2, 5, False, True, True, True, True)