import chromadb
import pandas as pd
from openai import OpenAI
from chromadb.utils import embedding_functions
import time
import json

class Embedding_function_names():
    names = ["all-MiniLM-L6-v2", "all-MiniLM-L12-v2", "all-distilroberta-v1", "all-mpnet-base-v2"]

def get_db(name):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    collection = db_client.get_collection(name=name)
    return collection

def create_db_zylbercweig_laski(name="Zylbercweig-LASKI", embedding_model_name = "all-distilroberta-v1", pre_process=False, print_progress=False, transliterate=True):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)
    collection = db_client.get_or_create_collection(name=name, embedding_function=embedding_function)
    if transliterate:
        zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    else:
        zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    laski_bool = laski.isna()
    for i, row in zylbercweig.iterrows():
        if print_progress and i%100 == 0:
            print("Zylbercweig", i)
        name = row["title"]
        if pre_process:
            name = first_sur(name)
        id = row["id"]
        add_name_to_db(collection, name, "Zylbercweig", str(id), i)
    for i, row in laski.iterrows():
        if laski_bool["id"][i]:
            continue
        if print_progress and i%100 == 0:
            print("LASKI", i)
        name = row["title"]
        id = row["id"]
        add_name_to_db(collection, name, "LASKI", id, i)
    return collection

def create_db_yad_vashem(name="Yad_Vashem", embedding_model_name="all-distilroberta-v1", print_progress=False):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_model_name)
    collection = db_client.get_or_create_collection(name=name, embedding_function=embedding_function)
    dataset = pd.read_csv("datasets/testset13-YadVAshemitaly/yv_italy.tsv", sep="\t")
    bool = dataset.isna()
    for i, row in dataset.iterrows():
        if print_progress and i%100 == 0:
            print("Yad Vashem", i)
        name = row["title"]
        id = str(row["id"])
        if not (bool["id"][i] or bool["title"][i]):
            add_name_to_db(collection, name, "Yad Vashem", id, i)
    print("finished")
    return collection

def add_name_to_db(collection, name, source, id, index):
    collection.upsert(documents=name, ids=id, metadatas=[{"source": source, "id": id, "index": index}])
    
def add_embedding_to_db(collection, embedding, name, source, id):
    collection.upsert(embeddings=embedding ,documents=name, ids=id, metadatas=[{"source": source}])

def query_db_by_name(collection, name, source="n/a", results_amount=3, include=["documents", "distances", "metadatas"]):
    if not source == "n/a":
        return collection.query(query_texts=name, n_results=results_amount, where={"source": source}, include=include)
    else:
        return collection.query(query_texts=name, n_results=results_amount, include=include)

def query_db_by_embedding(collection, embedding, source="n/a", results_amount=3, include=["documents", "distances", "metadatas"]):
    if not source == "n/a":
        return collection.query(query_embeddings=embedding, n_results=results_amount, where={"source": source}, include=include)
    else:
        return collection.query(query_embeddings=embedding, n_results=results_amount, include=include)

def query_db_by_name_singular_dataset(collection, name, id, results_amount=3, include=["documents", "distances", "metadatas"]):
    return collection.query(query_texts=name, n_results=results_amount, where={"id": {"$ne": id}}, include=include)
    
def first_sur(name):
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
    return name
    
def calc_db_recall_multiple_datasets(collection, matches_path="datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv",
                   candidates=5, pre_process=False, print_progress=False, logging=False, transliterate=True):
    print(f'Starting recall calculations on the {collection.name} model, with {candidates} canditates per match and first/surname == {pre_process}')
    t = time.time()
    dataset = pd.read_csv(matches_path, sep="\t")
    match_total, match_laski, match_zylbercweig = False, False, False
    comparisons, matches_total, matches_laski, matches_zylbercweig = 0, 0, 0, 0
    id_table, id_laski, id_zylbercweig, dist_laski, dist_zylbercweig, dists_zylbercweig, dists_laski = [], [], [], [], [], [], []
    for _, row in dataset.iterrows():
        if print_progress and _%50 == 0:
            print(_, "out of:", len(dataset))
        if transliterate:
            zylbercweig = row["transliterated_name"]
        else:
            zylbercweig = row["Zylbercweig Name"]
        if pre_process:
            zylbercweig = first_sur(zylbercweig)
        laski = row["title"]
        id = row["key"]
        query_zylbercweig = query_db_by_name(collection, zylbercweig, "LASKI", candidates)
        query_laski = query_db_by_name(collection, laski, "Zylbercweig", candidates)
        match_total = False
        match_laski = False
        match_zylbercweig = False
        index_laski = -1
        index_zylbercweig = -1
        for i, name in enumerate(query_zylbercweig["documents"][0]):
            if name == laski:
                match_zylbercweig = True
                match_total = True
                index_zylbercweig = i
        for i, name in enumerate(query_laski["documents"][0]):
            if name == zylbercweig:
                match_laski = True
                match_total = True
                index_laski = i
        if match_total == True:
            matches_total += 1
            id_table.append(id)
            if match_zylbercweig == True:
                matches_zylbercweig += 1
                id_zylbercweig.append(id)
                dist_zylbercweig.append(query_zylbercweig["distances"][0][index_zylbercweig])
                dists_zylbercweig.append(query_zylbercweig["distances"][0])
                index_zylbercweig = -1
            if match_laski == True:
                matches_laski += 1
                id_laski.append(id)
                dist_laski.append(query_laski["distances"][0][index_laski])
                dists_laski.append(query_laski["distances"][0])
                index_laski = -1
        comparisons += 1
    recall_laski = matches_laski/comparisons
    recall_zylbercweig = matches_zylbercweig/comparisons
    recall_total = matches_total/comparisons
    log_string = f'modelname: {collection.name}\n' + f'Total matches checked: {comparisons} with {candidates} candidates per match\n' + f'Recall LASKI: {recall_laski:.4f}\n' + f'Recall zylbercweig: {recall_zylbercweig:.4f}\n' + f'Recall total: {recall_total:.4f}'
    if logging:
        f = open("db/vectordb.chroma/logfile_recall.txt", "a")
        f.write(log_string + "\n\n")
        f.close()
    else:
        print(log_string)
    print(f'finished in {time.time()-t} seconds')
    return id_table, id_laski, id_zylbercweig, dist_laski, dist_zylbercweig, dists_laski, dists_zylbercweig

def calc_db_recall_singular_dataset(collection, matches_path="datasets/testset13-YadVAshemitaly/em.tsv", candidates=5, print_progress=False, logging=False):
    print(f'Starting recall calculations on the {collection.name} model, with {candidates} canditates per match')
    t = time.time()
    dataset = pd.read_csv(matches_path, sep="\t")
    matches = 0
    for i, row in dataset.iterrows():
        if print_progress and i % 50 == 0:
            print(i, "out of:", len(dataset))
        name_1 = row["title_1"]
        name_2 = row["title_2"]
        id_1 = row["id_1"]
        id_2 = row["id_2"]
        query_1 = query_db_by_name_singular_dataset(collection, name_1, id_1, candidates)
        query_2 = query_db_by_name_singular_dataset(collection, name_2, id_2, candidates)
        match = False
        for name in query_1["documents"][0]:
            if name == name_2:
                match = True
                break
        if match == False:
            for name in query_2["documents"][0]:
                if name == name_1:
                    match = True
        if match == True:
            matches += 1
    recall = matches/len(dataset)
    log_string = f'Modelname: {collection.name}\n' + f'Total matches checked: {len(dataset)} with {candidates} candidates per match\n' + f'Recall: {recall:.4f}'
    if logging:
        f = open("db/vectordb.chroma/logfile_recall.txt", "a")
        f.write(log_string + "\n\n")
        f.close()
    else:
        print(log_string)
    print(f'finished in {time.time()-t} seconds')

def find_model_similarity(model1, model2, candidates=5 ,model1_pre_process=False, model2_pre_process=False, print_progress=False, logging_similarity=False,
                          logging_recall=False, em_path="datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv", transliterate=True):
    # Calculates the Jaccard similarity between 2 models
    id_table1 = calc_db_recall_multiple_datasets(model1, em_path, candidates=candidates, pre_process=model1_pre_process, print_progress=print_progress, logging=logging_recall, transliterate=transliterate)
    id_table2 = calc_db_recall_multiple_datasets(model2, em_path, candidates=candidates, pre_process=model2_pre_process, print_progress=print_progress, logging=logging_recall, transliterate=transliterate)
    matches = 0
    for id in id_table1:
        if id in id_table2:
            matches += 1
    total_ids = len(id_table1) + len(id_table2) - matches
    similarity = matches/total_ids
    log_string = f'Jaccard similarity between {model1.name} and {model2.name} with {candidates} candidates per query: {matches}/{total_ids}={similarity:.4f}'
    if logging_similarity:
        f = open("db/vectordb.chroma/logfile_jaccard_similarity.txt", "a")
        f.write(log_string + "\n\n")
        f.close()
    else:
        print(log_string)

def get_openai_embedding(name, model):
    with open("secrets.json", "r") as file:
        secrets = json.load(file)
    client = OpenAI(
        organization=secrets["organization"],
        project=secrets["project"],
        api_key=secrets["api_key"],
    )
    return client.embeddings.create(input=name, model=model).data[0].embedding

def create_openai_embedding_csv(model):
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    id, name, embedding = [], [], []
    for _, row in zylbercweig.iterrows():
        id.append(row["id"])
        name.append(row["title"])
        embedding.append(get_openai_embedding(row["title"], model))
    for _, row in zylbercweig.iterrows():
        id.append(str(row["id"]) + "_first_sur")
        name_temp = first_sur(row["title"])
        name.append(name_temp)
        embedding.append(get_openai_embedding(name_temp, model))
    for _, row in laski.iterrows():
        id.append(row["id"])
        name.append(row["title"])
        embedding.append(get_openai_embedding(row["title"], model))
    data = pd.DataFrame({
        "id": id,
        "title": name,
        "embedding": embedding
    })
    data.to_csv(f"datasets/testset15-Zylbercweig-Laski/embeddings_{model}.csv", sep="\t")

def load_openai_embeddings(path):
    data = pd.read_csv(path, sep="\t")
    ids, embeddings, names = [], [], []
    for _, row in data.iterrows():
        ids.append(row["id"])
        embeddings.append(row["embedding"])
        names.append(row["title"])
    return ids, embeddings, names

def create_openai_embedding_database(embeddings_path, name="Zylbercweig-LASKI", embedding_model_name = "text-embedding-3-small", pre_process = False):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    if pre_process:
        collection = db_client.get_or_create_collection(name=name+embedding_model_name+"first_sur")
    else:
        collection = db_client.get_or_create_collection(name=name+embedding_model_name)
    ids, embeddings, names = load_openai_embeddings(embeddings_path)
    if pre_process:
        for i in enumerate(embeddings[0:2334]):
            add_embedding_to_db(collection, embeddings[i], names[i], "Zylbercweig", ids[i])
    else:
        for i in enumerate(embeddings[2334:4668]):
            add_embedding_to_db(collection, embeddings[i], names[i], "Zylbercweig", ids[i])
    for i in enumerate(embeddings[4668:6000]):
        add_embedding_to_db(collection, embeddings[i], names[i], "LASKI", ids[i])
    return collection

def find_experiment_dists(collection, candidates, pre_process, print_progress, matches_path="datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv"):
    print(f'Starting experiment calculations on the {collection.name} model, with {candidates} canditates per match and first/surname == {pre_process}')
    t = time.time()
    dataset = pd.read_csv(matches_path, sep="\t")
    match_laski, match_zylbercweig = False, False
    comparisons = 0
    id_table, dist_laski, dist_zylbercweig, dists_zylbercweig, dists_laski = [], [], [], [], []
    for _, row in dataset.iterrows():
        if print_progress and _%50 == 0:
            print(_, "out of:", len(dataset))
        zylbercweig = row["transliterated_name"]
        if pre_process:
            zylbercweig = first_sur(zylbercweig)
        laski = row["title"]
        id = row["key"]
        query_zylbercweig = query_db_by_name(collection, zylbercweig, "LASKI", candidates)
        query_laski = query_db_by_name(collection, laski, "Zylbercweig", candidates)
        match_laski = False
        match_zylbercweig = False
        index_laski = -1
        index_zylbercweig = -1
        for i, name in enumerate(query_zylbercweig["documents"][0]):
            if name == laski:
                match_zylbercweig = True
                index_zylbercweig = i
        for i, name in enumerate(query_laski["documents"][0]):
            if name == zylbercweig:
                match_laski = True
                index_laski = i
        id_table.append(id)
        dists_zylbercweig.append(query_zylbercweig["distances"][0])
        dists_laski.append(query_laski["distances"][0])
        if match_zylbercweig == True:
            dist_zylbercweig.append(query_zylbercweig["distances"][0][index_zylbercweig])
        else:
            dist_zylbercweig.append(9999)
        index_zylbercweig = -1
        if match_laski == True:
            dist_laski.append(query_laski["distances"][0][index_laski])
        else:
            dist_laski.append(9999)
        index_laski = -1
        comparisons += 1
    print(f'finished in {time.time()-t} seconds')
    return id_table, dist_laski, dist_zylbercweig, dists_laski, dists_zylbercweig

def prepare_experiments(candidates, print_progress, model_names, functions, pre_process, output_path="datasets/testset15-Zylbercweig-Laski/experiments.csv"):
    db_client = chromadb.PersistentClient(path="./db/vectordb.chroma")
    ids, names, candidate_array, dists_laski, dists_zylbercweig, dists_query_laski, dists_query_zylbercweig = [], [], [], [], [], [], []
    for i, _ in enumerate(model_names):
        embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=functions[i])
        collection = db_client.get_collection(name=model_names[i], embedding_function=embedding_function)
        id_table, dist_laski, dist_zylbercweig, dist_query_laski, dist_query_zylbercweig = find_experiment_dists(
        collection, candidates=candidates, pre_process=pre_process[i], print_progress=print_progress)
        ids.append(id_table)
        dists_laski.append(dist_laski)
        dists_zylbercweig.append(dist_zylbercweig)
        dists_query_laski.append(dist_query_laski)
        dists_query_zylbercweig.append(dist_query_zylbercweig)
        names.append(model_names[i])
        candidate_array.append(candidates)
        print("\n\n")
    data = pd.DataFrame({
        "model_name": names,
        "ids": ids,
        "dist_match_zylbercweig": dists_zylbercweig,
        "dist_match_laski": dists_laski,
        "dists_query_zylbercweig": dists_query_zylbercweig,
        "dists_query_laski": dists_query_laski,
        "candidates": candidate_array
    })
    data.to_csv(output_path, sep="\t")
    return names, ids, dists_zylbercweig, dists_laski, dists_query_zylbercweig, dists_query_laski, candidate_array

def prep_all_experiments(candidates):
    model_names = []
    functions = []
    pre_process = []
    for model in Embedding_function_names.names:
        model_names.append("Zylbercweig-LASKI" + model)
        model_names.append("Zylbercweig-LASKI" + model + "first-sur")
        functions.append(model)
        functions.append(model)
        pre_process.append(False)
        pre_process.append(True)
    return prepare_experiments(candidates, True, model_names, functions, pre_process)

def run_experiments(candidates):
    names, ids, dist_match_zylbercweig, dist_match_laski ,zylbercweig_dists, laski_dists, array = prep_all_experiments(candidates)
    name1_list, name2_list, matches, recall = [], [], [], []
    for i, name1 in enumerate(names):
        for j, name2 in enumerate(names):
            if j > i:
                name1_list.append(name1)
                name2_list.append(name2)
                id_table = []
                for k, id in enumerate(ids[i]):
                    laski_dist_list1 = laski_dists[i][k][0:candidates]
                    laski_dist_list2 = laski_dists[j][k][0:candidates]
                    zylbercweig_dist_list1 = zylbercweig_dists[i][k][0:candidates]
                    zylbercweig_dist_list2 = zylbercweig_dists[j][k][0:candidates]
                    laski_dists_final = []
                    zylbercweig_dists_final = []
                    for _ in range(0, candidates):
                        if laski_dist_list1[0] > laski_dist_list2[0]:
                            laski_dists_final.append(laski_dist_list2[0])
                            laski_dist_list2 = laski_dist_list2[1:len(laski_dist_list2)]
                        else:
                            laski_dists_final.append(laski_dist_list1[0])
                            laski_dist_list1 = laski_dist_list1[1:len(laski_dist_list1)]
                        if zylbercweig_dist_list1[0] > zylbercweig_dist_list2[0]:
                            zylbercweig_dists_final.append(zylbercweig_dist_list2[0])
                            zylbercweig_dist_list2 = zylbercweig_dist_list2[1:len(zylbercweig_dist_list2)]
                        else:
                            zylbercweig_dists_final.append(zylbercweig_dist_list1[0])
                            zylbercweig_dist_list1 = zylbercweig_dist_list1[1:len(zylbercweig_dist_list1)]
                        final_dist = laski_dists_final[len(laski_dists_final)-1]
                    if dist_match_laski[i][k] <= final_dist or dist_match_zylbercweig[i][k] <= final_dist or dist_match_laski[j][k] <= final_dist or dist_match_zylbercweig[j][k] <= final_dist:
                        id_table.append(id)
                matches.append(id_table)
                recall.append(len(id_table)/len(ids[0]))
    dataframe = pd.DataFrame({
        "model_1": name1_list,
        "model_2": name2_list,
        "recall": recall
    })
    dataframe.to_csv(f"datasets/testset15-Zylbercweig-Laski/experiment_results_{candidates}.csv", sep="\t")


def run_filtering_multiple_dataset(name_list_1, name_list_2, candidates=200, dataset_1_source="Zylbercweig", dataset_2_source="LASKI", model="Zylbercweig-LASKIall-distilroberta-v1", print_progress=False):
    collection = get_db(model)
    results_1, results_2 = [], []
    for i, name in enumerate(name_list_1):
        if print_progress and i%100 == 0:
            print("list 1:", i, "out of", len(name_list_1))
        results_1.append(query_db_by_name(collection, name, dataset_2_source, candidates))
    for i, name in enumerate(name_list_2):
        if print_progress and i%100 == 0:
            print("list 2:", i, "out of", len(name_list_1))
        results_2.append(query_db_by_name(collection, name, dataset_1_source, candidates))
    return results_1, results_2

def run_filtering_singular_dataset(name_list, id_list, candidates=200, model="Zylbercweig-LASKIall-distilroberta-v1", print_progress=False):
    collection = get_db(model)
    results = []
    for i, name in enumerate(name_list):
        if print_progress and i%100 == 0:
            print(i, "out of", len(name_list))
        results.append(query_db_by_name_singular_dataset(collection, name, id_list[i], candidates))
    return results

def create_dict_from_blocks(blocks, file_name="n/a"):
    dict = {}
    for i, singular_block in enumerate(blocks):
        table = []
        for metadata in singular_block["metadatas"][0]:
            table.append(metadata["index"])
        dict.update({i:table})
    if file_name != "n/a":
        with open(f"datasets/testset15-Zylbercweig-Laski/dicts/{file_name}.json", "w") as file:
            json.dump(dict, file, ensure_ascii=False, indent=4)
    return dict

def run_with_blocks_zylbercweig_laski(blocks, embedding_function="all-distilroberta-v1", candidates=10, file_name="n/a"):
    t = time.time()
    model = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_function)
    zylbercweig_dict = {}
    laski_dict = {}
    result = {}
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    db_client = chromadb.EphemeralClient()
    for i, row in zylbercweig.iterrows():
        if i%100 == 0:
            print("embedded", i, "out of", len(zylbercweig), "in Zylbercweig")
        embedding = model(row["title"])[0]
        zylbercweig_dict.update({i:embedding})
    for i, row in laski.iterrows():
        if i%100 == 0:
            print("embedded", i, "out of", len(laski), "in LASKI")
        embedding = model(row["title"])[0]
        laski_dict.update({i:embedding})
    for i in range(0, len(laski)-1):
        if i%100 == 0:
            print("filtered", i, "out of", len(laski), "blocks")
        collection = db_client.create_collection(name=f"temp{i}", embedding_function=model)
        embeddings = []
        ids = []
        for index in blocks.get(i):
            embeddings.append(zylbercweig_dict[index])
            ids.append(str(index))
        collection.add(ids=ids, embeddings=embeddings)
        query_result = collection.query(query_embeddings=laski_dict.get(i), n_results=candidates)
        result.update({i:query_result["ids"][0]})
    if file_name != "n/a":
        with open(f"datasets/testset15-Zylbercweig-Laski/dicts/{file_name}.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)
    print(f'finished in {(time.time()-t)/60} minutes')
    return result

def run_with_blocks_yad_vashem(blocks, embedding_function="all-distilroberta-v1", candidates=10, file_name="n/a"):
    t = time.time()
    model = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=embedding_function)
    dict = {}
    result = {}
    dataset = pd.read_csv("datasets/testset13-YadVAshemitaly/yv_italy.tsv", sep="\t")
    db_client = chromadb.EphemeralClient()
    yad_bool = dataset.isna()
    for i, row in dataset.iterrows():
        if i%100 == 0:
            print("embedded", i, "out of", len(dataset))
        if yad_bool["title"][i]:
            continue
        embedding = model(row["title"])[0]
        dict.update({i:embedding})
    for i in range(0, len(dict)-1):
        if i%100 == 0:
            print(i, "out of", len(dict), "in filtering")
        collection = db_client.create_collection(name=f"temp{i}", embedding_function=model)
        embeddings = []
        ids = []
        for index in blocks.get(i):
            embeddings.append(dict[index])
            ids.append(str(index))
        collection.add(ids=ids, embeddings=embeddings)
        query_result = collection.query(query_embeddings=dict.get(i), n_results=candidates, where={"index":{"$ne":str(i)}})
        result.update({i:query_result["ids"][0]})
    if file_name != "n/a":
        with open(f"datasets/testset13-YadVAshemitaly/dicts/{file_name}.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)
    print(f'finished in {(time.time()-t)/60} minutes')
    return result

def run_filtering_from_blocks_zylbercweig_laski(block_path_laski, block_path_zylbercweig="n/a", candidates=100):
    laski = {}
    with open(block_path_laski) as file:
        laski = json.load(file)
        laski = {int(k): set(v) for k, v in laski.items()}
        run_with_blocks_zylbercweig_laski(laski, candidates=candidates, file_name="laski_vectordb")
    if block_path_zylbercweig != "n/a":
        zylbercweig = {}
        with open(block_path_zylbercweig) as file:
            zylbercweig = json.load(file)
            zylbercweig = {int(k): set(v) for k, v in zylbercweig.items()}
            run_with_blocks_zylbercweig_laski(zylbercweig, candidates=candidates, file_name="zylbercweig_vectordb")

def run_filtering_from_blocks_yad_vashem(block_path, candidates=100, file_name="yad_vashem_vectordb"):
    with open(block_path) as file:
        blocks = json.load(file)
        blocks = {int(k): set(v) for k, v in blocks.items()}
        run_with_blocks_yad_vashem(blocks, candidates=candidates, file_name=file_name)


if __name__ == "__main__":
    run_filtering_from_blocks_yad_vashem("app/filtered_blocks.json")