import pandas as pd
alphabet_yiddish = ["א", "אַַ", "אָָ", "ב", "בֿ", "ג", "ד", "ה", "ו", "וּ", "ז", "ח", "ט", "י", "כּ", "כ", "ך", "ל", "מ", "ם", "נ", "ן", "ס", "ע", "פּ", "פֿ", "ף", "צ", "ץ", "ק", "ר", "ש", "שׂ", "תּ", "ת"]
alphabet_roman = ["", "a", "o", "b", "v", "g", "d", "h", "u", "u", "z", "kh", "t", "y", "k", "kh", "kh", "i", "m", "m", "n", "n", "s", "e", "p", "f", "f", "ts", "ts", "k", "r", "sh", "s", "t", "s"]


def transliterate_zylbercweig(output_path):
    zylbercweig = pd.read_csv("testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t")
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
    output = pd.DataFrame({
        "id": id,
        "lon": lon,
        "geo_id": geo_id,
        "geowkt": geowkt,
        "lat": lat,
        "title": title,
        "geo_source": geo_source,
        "title_source": title_source
    })
    output.to_csv(output_path, sep="\t")

def transliterate_yiddish(input_string):
    input_string = input_string.replace("וו", "v")
    input_string = input_string.replace("דזש", "dzh")
    input_string = input_string.replace("זש", "zh")
    input_string = input_string.replace("טש", "tsh")
    input_string = input_string.replace("וי", "oy")
    input_string = input_string.replace("יי", "ey")
    input_string = input_string.replace("ײַ", "ay")
    for i, _ in enumerate(input_string):
        if input_string[i] == "א":
            input_string[i] == "a"
    for i, _ in enumerate(alphabet_yiddish):
        input_string = input_string.replace(alphabet_yiddish[i], alphabet_roman[i])
    return input_string

def transliterate_yiddish_new(input_string):
    transliterated_string = ""
    print(input_string[0])
    for i, _ in enumerate(input_string):
        for j, _ in enumerate(alphabet_yiddish):
            if input_string[i] == alphabet_yiddish[j]:
                print(alphabet_yiddish[j])
                print("i is: " + str(i) + " j is: " + str(j))
                transliterated_string = transliterated_string + alphabet_roman[j]
    return transliterated_string

if __name__ == "__main__":
    transliterate_zylbercweig("testset15-Zylbercweig-Laski/Zylbercweig_roman.csv")

