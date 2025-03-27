import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("\\tests", ""))
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("/tests", ""))
import app
import app.decision_tree as decision
import pandas as pd
class Test_transliterate_yiddish():
    def test_individual_letter(self):
        for i, _ in enumerate(app.Alphabet.yiddish):
            assert app.Alphabet.roman[i] == app.transliterate_yiddish(app.Alphabet.yiddish[i])
    
    def test_special_cases(self):
        for i, _ in enumerate(app.Alphabet.cases_yiddish):
            assert app.Alphabet.cases_roman[i] == app.transliterate_yiddish(app.Alphabet.cases_yiddish[i])

    def test_names(self):
        assert app.transliterate_yiddish("אַּבבּדול") == "abvdui"
        
class Test_Levenshtein():
    def test_base_cases(self):
        assert decision.levenshtein_distance("", "dsa") == 3
        assert decision.levenshtein_distance("", "dsa4") == 4
        assert decision.levenshtein_distance("", "dsa321") == 6
        assert decision.levenshtein_distance("dsa", "") == 3
        assert decision.levenshtein_distance("dsa4", "") == 4
        assert decision.levenshtein_distance("dsa321", "") == 6
    def test_same_string(self):
        assert decision.levenshtein_distance("", "") == 0
        assert decision.levenshtein_distance("house", "house") == 0
    def test_strings(self):
        assert decision.levenshtein_distance("ghost", "house") == 3

class test_name_distances():
    def test_name_distances(self):
        zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
        laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
        combinations = pd.read_csv("datasets/testset15-Zylbercweig-Laski/name_distances.csv", sep="\t")