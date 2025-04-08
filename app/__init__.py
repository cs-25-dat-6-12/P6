import pandas as pd

class Alphabet():
    yiddish = ["א", "אַַ", "אָָ", "ב", "בֿ", "ג", "ד", "ה", "ו", "וּ", "ז", "ח", "ט", "י", "כ", "כּ", "ך", "ל", "מ", "ם", "נ", "ן", "ס", "ע", "פּ", "פֿ", "ף", "צ", "ץ", "ק", "ר", "ש", "שׂ", "ת", "תּ", "פ", " ", "(", ")", "-", "\"", "{", "}", "ײ"]
    roman = ["", "a", "o", "b", "v", "g", "d", "h", "u", "u", "z", "kh", "t", "y", "kh", "k", "kh", "i", "m", "m", "n", "n", "s", "e", "p", "f", "f", "ts", "ts", "k", "r", "sh", "s", "s", "t", "", " ", "(", ")", "-", "\"", "{", "}", ""]
    cases_yiddish = ["וו", "דזש", "זש", "טש", "וי", "יי", "ײַ"]
    cases_roman = ["v", "dzh", "zh", "tsh", "oy", "ey", "ay"]

def transliterate_zylbercweig(output_path):
    zylbercweig = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t")
    id, lon, geo_id, geowkt, lat, title, geo_source, title_source, name_parts = [], [], [], [], [], [], [], [], []
    for _, row in zylbercweig.iterrows():
        id.append(row["id"].replace("temp_", ""))
        lon.append(row["lon"])
        geo_id.append(row["geo_id"])
        geowkt.append(row["geowkt"])
        lat.append(row["lat"])
        title.append(transliterate_yiddish(row["title"]))
        geo_source.append(row["geo_source"])
        title_source.append(row["title_source"])
        name_parts.append(transliterate_name_parts(row["name_parts"]))
    output = pd.DataFrame({
        "id": id,
        "lon": lon,
        "geo_id": geo_id,
        "geowkt": geowkt,
        "lat": lat,
        "title": title,
        "geo_source": geo_source,
        "title_source": title_source,
        "name_parts": name_parts
    })
    output.to_csv(output_path, sep="\t")

def transliterate_yiddish(input_string):
    transliterated_string = ""
    skip = 0
    for i, _ in enumerate(input_string):
        if skip > 0:
            skip = skip - 1
            continue
        for j, _ in enumerate(Alphabet.yiddish):
            if input_string[i] == Alphabet.yiddish[j]:
                if i < len(input_string)-1:
                    # testing for diacritics
                    if j == 0 and input_string[i+1] == "ַ":
                        transliterated_string = transliterated_string + Alphabet.roman[1]
                        continue
                    elif j == 0 and input_string[i+1] == "ָ":
                        transliterated_string = transliterated_string + Alphabet.roman[2]
                        continue
                    elif j == 3 and (input_string[i+1] == "ֿ"or input_string[i+1] == "ּ"):
                        transliterated_string = transliterated_string + Alphabet.roman[4]
                        continue
                    elif j == 8 and input_string[i+1] == "ּ":
                        transliterated_string = transliterated_string + Alphabet.roman[9]
                        continue
                    elif j == 14 and input_string[i+1] == "ּ":
                        transliterated_string = transliterated_string + Alphabet.roman[15]
                        continue
                    elif j == 35 and input_string[i+1] == "ּ":
                        transliterated_string = transliterated_string + Alphabet.roman[24]
                        continue
                    elif j == 35 and input_string[i+1] == "ֿ":
                        transliterated_string = transliterated_string + Alphabet.roman[25]
                        continue
                    elif j == 31 and input_string[i+1] == "ׂ":
                        transliterated_string = transliterated_string + Alphabet.roman[32]
                        continue
                    elif j == 33 and input_string[i+1] == "ּ":
                        transliterated_string = transliterated_string + Alphabet.roman[34]
                        continue
                    
                    # special cases
                    for k, _ in enumerate(Alphabet.cases_yiddish):
                        if Alphabet.yiddish[j] == Alphabet.cases_yiddish[k][0] and input_string[i+1] == Alphabet.cases_yiddish[k][1]:
                            if k != 1:
                                transliterated_string = transliterated_string + Alphabet.cases_roman[k]
                                skip = 1
                            elif i < len(input_string)-2 and input_string[i+2] == Alphabet.cases_yiddish[k][2]:
                                transliterated_string = transliterated_string + Alphabet.cases_roman[k]
                                skip = 2
                    if skip == 0:
                        transliterated_string = transliterated_string + Alphabet.roman[j]
                else:
                    transliterated_string = transliterated_string + Alphabet.roman[j]
    return transliterated_string

def transliterate_name_parts(input_string):
    commaseplist = input_string.split(", ")
    colonseplist = []
    for list in commaseplist:
        colonseplist.append(list.split(": "))
    for i, list in enumerate(colonseplist):
        for j, text in enumerate(list):
            if j % 2 == 1:
                colonseplist[i][j] = transliterate_yiddish(text)
    return_string = ""
    for i, list in enumerate(colonseplist):
        for j, text in enumerate(list):
            return_string = return_string + text
            if j == 0:
                return_string = return_string + ": "
            elif i != len(colonseplist) - 1:
                return_string = return_string + ", "
    return return_string

def writefunction(text):
    list = []
    for char in text:
        list.append(char)
    out = pd.DataFrame({
        "text": list
    })
    out.to_csv("test", sep="\t")
    
def write_transliterated_matches(output_path):
    matches = pd.read_csv("datasets/testset15-Zylbercweig-Laski/em.tsv", sep="\t")
    id_1, id_2, title_1, title_2, judgement = [], [], [], [], []
    for _, row in matches.iterrows():
        id_1.append(row["id_1"])
        id_2.append(row["id_2"])
        title_1.append(transliterate_name_parts(row["title_1"]))
        title_2.append(row["title_2"])
        judgement.append(row["judgement"])
    output = pd.DataFrame({
        "id_1": id_1,
        "id_2": id_2,
        "title_1": title_1,
        "title_2": title_2,
        "judgement": judgement
    })
    output.to_csv(output_path, sep="\t")
    

if __name__ == "__main__":
    transliterate_zylbercweig("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv")
    write_transliterated_matches("datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv")