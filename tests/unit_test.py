import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace("\\tests", ""))
from app import transliterate_yiddish_new
class Test_transliterate_yiddish():
    def test_individual_letter(self):
        alphabet_yiddish = ["א", "אַַ", "אָָ", "ב", "בֿ", "ג", "ד", "ה", "ו", "וּ", "ז", "ח", "ט", "י", "כּ", "כ", "ך", "ל", "מ", "ם", "נ", "ן", "ס", "ע", "פּ", "פֿ", "ף", "צ", "ץ", "ק", "ר", "ש", "שׂ", "תּ", "ת"]
        alphabet_roman = ["", "a", "o", "b", "v", "g", "d", "h", "u", "u", "z", "kh", "t", "y", "k", "kh", "kh", "i", "m", "m", "n", "n", "s", "e", "p", "f", "f", "ts", "ts", "k", "r", "sh", "s", "t", "s"]
        #print(alphabet_yiddish[1])
        #print(transliterate_yiddish_new(alphabet_yiddish[1]) + "here")
        for i, _ in enumerate(alphabet_yiddish):
            print(alphabet_yiddish[i] + " is")
            assert alphabet_roman[i] == transliterate_yiddish_new(alphabet_yiddish[i])