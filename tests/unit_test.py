import pytest
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("\\tests", ""))
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("/tests", ""))
import app
from app.vectordb import get_db, query_db_by_name_singular_dataset, get_db, query_db_by_name, run_blocking_multiple_dataset, create_dict_from_block, run_blocking_singular_dataset

class Test_transliterate_yiddish():
    def test_individual_letter(self):
        for i, _ in enumerate(app.Alphabet.yiddish):
            assert app.Alphabet.roman[i] == app.transliterate_yiddish(app.Alphabet.yiddish[i])
    
    def test_special_cases(self):
        for i, _ in enumerate(app.Alphabet.cases_yiddish):
            assert app.Alphabet.cases_roman[i] == app.transliterate_yiddish(app.Alphabet.cases_yiddish[i])

    def test_names(self):
        assert app.transliterate_yiddish("אַּבבּדול") == "abvdui"

class Test_vectordb():
    def test_get_and_query_db(self):
        collection = get_db("Zylbercweig-LASKIall-distilroberta-v1")
        results = []
        for i in range(20):
            results.append(query_db_by_name(collection, "test", results_amount=i+1))
        for i, result in enumerate(results):
            assert len(result["documents"][0]) == i+1

    def test_match_itself_Yad_Vashem(self):
        model = "Yad_Vashemall-distilroberta-v1"
        collection = get_db(model)
        name = "Mira Vamos Fridrikh"
        id = "1004394"
        query = query_db_by_name_singular_dataset(collection, name, id, 10)
        for id_temp in query["ids"][0]:
            assert id_temp != id

    def test_create_dict(self):
        result1, result2 = run_blocking_multiple_dataset(["test1", "test2", "test3", "test4", "test5"], ["test6", "test7", "test8", "test9", "test10"], 5)
        result3 = run_blocking_singular_dataset(["test11", "test12", "test13", "test14", "test15"], ["1", "2", "3", "4", "5"], 5)
        dict1 = create_dict_from_block(result1)
        dict2 = create_dict_from_block(result2)
        dict3 = create_dict_from_block(result3)
        dicts = [dict1, dict2, dict3]
        results = [result1, result2, result3]
        for i, result in enumerate(results):
            for j, block in enumerate(result):
                for metadata in block["metadatas"][0]:
                    assert metadata["index"] in dicts[i][j]