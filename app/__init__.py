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
    yiddish = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t")
    roman = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    id, title_yiddish, title_roman, title_LASKI, name_parts_yiddish, name_parts_roman, name_parts_LASKI = [], [], [], [], [], [], []
    bool_table = yiddish.isna()
    for i, row in yiddish.iterrows():
        if not bool_table["geo_source"][i]:
            source_string = row["geo_source"].replace("laski:", "")
            for j, row2 in laski.iterrows():
                if row2["id"] == source_string:
                    id.append(row["id"])
                    title_yiddish.append(row["title"])
                    title_roman.append(roman["title"][i])
                    title_LASKI.append(row2["title"])
                    name_parts_yiddish.append(row["name_parts"])
                    name_parts_roman.append(roman["name_parts"][i])
                    name_parts_LASKI.append(row2["name_parts"])
    output = pd.DataFrame({
        "id": id,
        "title_yiddish": title_yiddish,
        "title_roman": title_roman,
        "title_LASKI": title_LASKI,
        "name_parts_yiddish": name_parts_yiddish,
        "name_parts_roman": name_parts_roman,
        "name_parts_LASKI": name_parts_LASKI
    })
    output.to_csv(output_path, sep="\t")
    
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

if __name__ == "__main__":
    transliterate_zylbercweig("datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv")
    write_transliterated_matches("datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv")