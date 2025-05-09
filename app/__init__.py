import json
import pandas as pd


class Alphabet:
    yiddish = [
        "א",
        "אַַ",
        "אָָ",
        "ב",
        "בֿ",
        "ג",
        "ד",
        "ה",
        "ו",
        "וּ",
        "ז",
        "ח",
        "ט",
        "י",
        "כ",
        "כּ",
        "ך",
        "ל",
        "מ",
        "ם",
        "נ",
        "ן",
        "ס",
        "ע",
        "פּ",
        "פֿ",
        "ף",
        "צ",
        "ץ",
        "ק",
        "ר",
        "ש",
        "שׂ",
        "ת",
        "תּ",
        "פ",
        " ",
        "(",
        ")",
        "-",
        '"',
        "{",
        "}",
        "ײ",
    ]
    roman = [
        "",
        "a",
        "o",
        "b",
        "v",
        "g",
        "d",
        "h",
        "u",
        "u",
        "z",
        "kh",
        "t",
        "y",
        "kh",
        "k",
        "kh",
        "i",
        "m",
        "m",
        "n",
        "n",
        "s",
        "e",
        "p",
        "f",
        "f",
        "ts",
        "ts",
        "k",
        "r",
        "sh",
        "s",
        "s",
        "t",
        "",
        " ",
        "(",
        ")",
        "-",
        '"',
        "{",
        "}",
        "",
    ]
    cases_yiddish = ["וו", "דזש", "זש", "טש", "וי", "יי", "ײַ"]
    cases_roman = ["v", "dzh", "zh", "tsh", "oy", "ey", "ay"]


def transliterate_zylbercweig(output_path):
    zylbercweig = pd.read_csv(
        "datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t"
    )
    id, lon, geo_id, geowkt, lat, title, geo_source, title_source, name_parts = (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
    )
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
    output = pd.DataFrame(
        {
            "id": id,
            "lon": lon,
            "geo_id": geo_id,
            "geowkt": geowkt,
            "lat": lat,
            "title": title,
            "geo_source": geo_source,
            "title_source": title_source,
            "name_parts": name_parts,
        }
    )
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
                if i < len(input_string) - 1:
                    # testing for diacritics
                    if j == 0 and input_string[i + 1] == "ַ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[1]
                        )
                        continue
                    elif j == 0 and input_string[i + 1] == "ָ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[2]
                        )
                        continue
                    elif j == 3 and (
                        input_string[i + 1] == "ֿ" or input_string[i + 1] == "ּ"
                    ):
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[4]
                        )
                        continue
                    elif j == 8 and input_string[i + 1] == "ּ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[9]
                        )
                        continue
                    elif j == 14 and input_string[i + 1] == "ּ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[15]
                        )
                        continue
                    elif j == 35 and input_string[i + 1] == "ּ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[24]
                        )
                        continue
                    elif j == 35 and input_string[i + 1] == "ֿ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[25]
                        )
                        continue
                    elif j == 31 and input_string[i + 1] == "ׂ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[32]
                        )
                        continue
                    elif j == 33 and input_string[i + 1] == "ּ":
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[34]
                        )
                        continue

                    # special cases
                    for k, _ in enumerate(Alphabet.cases_yiddish):
                        if (
                            Alphabet.yiddish[j] == Alphabet.cases_yiddish[k][0]
                            and input_string[i + 1] == Alphabet.cases_yiddish[k][1]
                        ):
                            if k != 1:
                                transliterated_string = (
                                    transliterated_string + Alphabet.cases_roman[k]
                                )
                                skip = 1
                            elif (
                                i < len(input_string) - 2
                                and input_string[i + 2] == Alphabet.cases_yiddish[k][2]
                            ):
                                transliterated_string = (
                                    transliterated_string + Alphabet.cases_roman[k]
                                )
                                skip = 2
                    if skip == 0:
                        transliterated_string = (
                            transliterated_string + Alphabet.roman[j]
                        )
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
    out = pd.DataFrame({"text": list})
    out.to_csv("test", sep="\t")


def write_transliterated_em(output_path):
    # fmt: off
    em = pd.read_csv("datasets/testset15-Zylbercweig-Laski/em_new.tsv", sep="\t")
    yiddish = pd.read_csv("datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t")
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    # fmt: on

    # the indexes of the matches we should drop because we can't find their corresponding entries in Zylbercweig and/or LASKI
    matches_to_drop = []

    # add the new columns to the dataframe
    em["index_roman"] = pd.Series()
    em["index_LASKI"] = pd.Series()
    em["name_parts_roman"] = pd.Series()
    em["name_parts_LASKI"] = pd.Series()

    for index, row in em.iterrows():
        # for every row in the matches file, find the entries it's made of in Zylbercweig and LASKI and add their indexes to the match row

        yiddish_mask = yiddish["title"] == row["Zylbercweig Name"]
        laski_mask = laski["id"] == row["id"]

        if laski_mask.sum() == 0 or yiddish_mask.sum() == 0:
            print(
                f"Dropping match with key {row["key"]}: Missing entry in LASKI or Zylbercweig."
            )
            matches_to_drop.append(index)
            continue
        if laski_mask.sum() + yiddish_mask.sum() > 2:
            print(
                f"Dropping match with key {row["key"]}: LASKI entries with same ID: {laski_mask.sum()}. Zylbercweig entries with same name: {yiddish_mask.sum()}"
            )
            matches_to_drop.append(index)
            continue

        # NOTE: .idxmax() is used to make sure we only get a single element out, which is fine since there should be at most 1
        yiddish_index = yiddish_mask.idxmax()
        laski_index = laski_mask.idxmax()

        # fmt: off
        em["index_roman"].iat[index] = yiddish.index[yiddish_index]
        em["index_LASKI"].iat[index] = laski.index[laski_index]
        em["name_parts_roman"].iat[index] = transliterate_name_parts(yiddish["name_parts"][yiddish_index])
        em["name_parts_LASKI"].iat[index] = laski["name_parts"][laski_index]
        # fmt: on

    # transliterate, drop the matches we couldn't find entries for, and write to the output_path
    em = em.drop(index=matches_to_drop)
    em.to_csv(output_path, sep="\t")


def write_indexed_italy_em(output_path):
    # this method will write the indexes of the records in matching pairs into the corresponding match and write the updated data to a file
    em = pd.read_csv(r"datasets\testset13-YadVAshemItaly\em.tsv", sep="\t")
    italy = pd.read_csv(r"datasets\testset13-YadVAshemItaly\yv_italy.tsv", sep="\t")
    # the indexes from records in the respective datasets, stored in the order they should appear
    indexes_1 = []
    indexes_2 = []
    for _, row in em.iterrows():
        mask_1 = (italy["id"] == row["id_1"]).idxmax()
        mask_2 = (italy["id"] == row["id_2"]).idxmax()
        indexes_1.append(italy.index[mask_1])
        indexes_2.append(italy.index[mask_2])

    indexes_1 = pd.Series(indexes_1)
    indexes_2 = pd.Series(indexes_2)

    em.insert(loc=0, column="index_2", value=indexes_2)
    em.insert(loc=0, column="index_1", value=indexes_1)

    em.to_csv(output_path, sep="\t")


def add_name_parts_from_title(input_filepath, output_filepath):
    df = pd.read_csv(input_filepath, sep=",")
    df["name_parts"] = pd.Series()
    for i, row in df.iterrows():
        df["name_parts"].iat[i] = json.dumps({
            f"name_part_{index}": name_part
            for index, name_part in enumerate(row["title"].split(" "))
        })
    df.to_csv(output_filepath, sep=",")


# FIXME this is outdated and should probably be removed
def write_transliterated_matches(output_path):
    yiddish = pd.read_csv(
        "datasets/testset15-Zylbercweig-Laski/Zylbercweig.tsv", sep="\t"
    )
    roman = pd.read_csv(
        "datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv", sep="\t"
    )
    laski = pd.read_csv("datasets/testset15-Zylbercweig-Laski/LASKI.tsv", sep="\t")
    (
        id,
        title_yiddish,
        title_roman,
        title_LASKI,
        name_parts_yiddish,
        name_parts_roman,
        name_parts_LASKI,
    ) = ([], [], [], [], [], [], [])
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
    output = pd.DataFrame(
        {
            "id": id,
            "title_yiddish": title_yiddish,
            "title_roman": title_roman,
            "title_LASKI": title_LASKI,
            "name_parts_yiddish": name_parts_yiddish,
            "name_parts_roman": name_parts_roman,
            "name_parts_LASKI": name_parts_LASKI,
        }
    )
    output.to_csv(output_path, sep="\t")


if __name__ == "__main__":
    # transliterate_zylbercweig(
    #    "datasets/testset15-Zylbercweig-Laski/Zylbercweig_roman.csv"
    # )
    # write_transliterated_matches(
    #    "datasets/testset15-Zylbercweig-Laski/Transliterated_matches.csv"
    # )

    # write_transliterated_em(
    #    "datasets/testset15-Zylbercweig-Laski/transliterated_em.csv"
    # )

    add_name_parts_from_title(
        "datasets/phonetic/phoneticZylbercweig_phonetic.csv",
        "datasets/phonetic/phoneticZylbercweig_name_parts.csv",
    )

    # write_indexed_italy_em(
    #    r"datasets\testset13-YadVAshemItaly\em_indexes.tsv",
    # )
