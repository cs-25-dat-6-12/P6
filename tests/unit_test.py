import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("\\tests", ""))
from app import *
class Test_transliterate_yiddish():
    def test_individual_letter(self):
        for i, _ in enumerate(Alphabet.yiddish):
            assert Alphabet.roman[i] == transliterate_yiddish(Alphabet.yiddish[i])
    
    def test_special_cases(self):
        for i, _ in enumerate(Alphabet.cases_yiddish):
            assert Alphabet.cases_roman[i] == transliterate_yiddish(Alphabet.cases_yiddish[i])

    def test_names(self):
        assert transliterate_yiddish("אַּבבּדול") == "abvdui"