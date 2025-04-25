import pytest
import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("\\tests", ""))
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("/tests", ""))
import app
from app.vectordb import get_db, query_db_by_name_singular_dataset

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
    def test_match_itself_Yad_Vashem(self):
        model = "Yad_Vashemall-distilroberta-v1"
        collection = get_db(model)
        dataset = pd.read_csv("datasets/testset13-YadVAshemitaly/yv_italy.tsv", sep="\t")
        bool = dataset.isna()
        for i, row in dataset.iterrows():
            name = row["title"]
            id = row["id"]
            if bool["id"][i] or bool["title"][i]:
                break
            query = query_db_by_name_singular_dataset(collection, name, id, 10)
            for id_temp in query["ids"][0]:
                assert id_temp != id