from openai import OpenAI
import json
import pandas as pd

test_name_LASKI = '{"given-name": "Zigmund", "surname": "Ehrlich"}'
test_name_Zylbercweig = '{"given-name": "Zygmund", "surname": "Ehriykh"}'
# test_name_LASKI = '{"given-name": "Chaim", "surname": "Abramovitsh"}'
# test_name_Zylbercweig = '{"given-name": "shium-yekb", "surname": "abramovytsh"}'
test_name_LASKI = '{"1": "Altman", "2": "Natan"}'
test_name_Zylbercweig = '{"1": "אַלטמאַן", "2": "נאַטאַן"}'
test_name_Zylbercweig = '{"1": "שמואל", "2": "אַלמאַן"}'

test_name_list = "Zigmund Ehrlich, Zygmund Ehriykh\nChaim Abramovitsh, shium-yekb abramovytsh\nIsadore Appel, aei yzydor\nNathan Birnbaum, nkhum byrnvoym"


class Developer_strings:
    developer_string1 = 'You are a language model that does entity resolution, You will be given two names split up into name parts, one from the Zylbercweig dataset and one from the LASKI dataset. The Zylbercweig dataset contains names written in the hebrew alphabet which have been transliterated into the roman alphabet, while the LASKI dataset contains names written in the roman alphabet. Your task is to determine whether the two names refer to the same person. Reply only with "True" or "False"'
    developer_string2 = 'You will be given two Python dictionaries containing name parts. Reply with "True" if the name parts in both dictionaries refer the same person and "False" otherwise. The name parts may refer to the same person even if their spelling is not exactly the same. Explain your answer in 30 words or less.'
    developer_string3 = 'You will be given two names. The first name has been transliterated from the hebrew alphabet and may be different from the second name even if the names refer to the same person. Your task is to determine if the names refer to the same person and respond with "True" if they do and "False" otherwise. Explain your answer in 30 words or less.'
    developer_string4 = 'You will be given a list of jewish name pairs with one pair on each line of the input. For each pair of names you must determine if they refer to the same person. One name in each pair name has been transliterated from the hebrew alphabet and may be different from the second name even if the names refer to the same person. If the names in a pair refer to the same person, write ", True" on the same line as the pair and ", False" on that line otherwise.'
    developer_string5 = 'You will be given two names. The first name is written in the hebrew alphabet and the other name is written in the roman alphabet. Your task is to determine if the names refer to the same person and respond with "True" if they do and "False" otherwise. Explain your answer in 30 words or less.'
    developer_string6 = 'You will be given a list of jewish name pairs with one pair on each line of the input. For each pair of names you must determine if they refer to the same person. The second name in each pair name has been transliterated from the hebrew alphabet and may be different from the second name even if the names refer to the same person. For each pair, replace it with "True" or "False" depending on whether or not the names refer to the same person. You provide an answer for all 12 pairs.'  # NOTE this prompt is currently hard-coded to 12 name pairs to fit the proofOfConcept dataset!


def prompt(client, developer_prompt, user_prompt, tokens=200):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "developer",
                "content": [{"type": "text", "text": developer_prompt}],
            },
            {"role": "user", "content": [{"type": "text", "text": user_prompt}]},
        ],
        max_tokens=tokens,
    )
    return completion.choices[0].message.content


def string_from_name_parts(name_parts):
    return_string = "{"
    for entry in name_parts:
        return_string += entry
        return_string += ": "
        return_string += name_parts[entry]
        return_string += ", "
    return_string = return_string[0 : len(return_string) - 2]
    return_string += "}"
    return return_string


def name_from_name_parts(name_parts):
    return_string = ""
    for entry in name_parts:
        return_string += name_parts[entry]
        return_string += " "
    return_string = return_string[:-1]
    return return_string


def create_prompt_string(Zylbercweig_name_parts, LASKI_name_parts, pattern=0):
    match pattern:
        case 0:
            return (
                "Do these two names refer to the same person?\n"
                + "Name from the Zylbercweig dataset: "
                + string_from_name_parts(Zylbercweig_name_parts)
                + "\nName from the LASKI dataset: "
                + string_from_name_parts(LASKI_name_parts)
            )
        case 1:
            return (
                string_from_name_parts(Zylbercweig_name_parts)
                + " "
                + string_from_name_parts(LASKI_name_parts)
            )
        case 2:
            return (
                "First name: "
                + name_from_name_parts(Zylbercweig_name_parts)
                + " Second name: "
                + name_from_name_parts(LASKI_name_parts)
            )
        case _:
            assert False  # we should never reach this case


def test_single_prompt():
    developer_string = Developer_strings.developer_string6
    prompt_string = create_prompt_string(
        json.loads(test_name_Zylbercweig), json.loads(test_name_LASKI), pattern=2
    )
    with open("secrets.json", "r") as file:
        secrets = json.load(file)
    client = OpenAI(
        organization=secrets["organization"],
        project=secrets["project"],
        api_key=secrets["api_key"],
    )
    # answer = prompt(client, developer_string, prompt_string, 200)
    answer = prompt(client, developer_string, test_name_list, 200)
    print(f"Developer:\n{developer_string}\nUser:\n{test_name_list}\nAnswer:\n{answer}")


def test_on_dataset(filepath):
    # load dataset
    print("Loading...")
    df = pd.read_csv(
        filepath,
        sep="\t",
        skiprows=1,
        names=["name_parts_LASKI", "name_parts_roman", "match"],
    )
    # transform name parts dictionary strings into names:
    print("Transforming...")
    df["name_parts_LASKI"] = (
        df["name_parts_LASKI"].apply(json.loads).apply(name_from_name_parts)
    )
    df["name_parts_roman"] = (
        df["name_parts_roman"].apply(json.loads).apply(name_from_name_parts)
    )
    print("Writing prompt...")
    # compose name list
    name_list = ""
    for _, row in df.iterrows():
        name_list += row["name_parts_LASKI"]
        name_list += ", "
        name_list += row["name_parts_roman"]
        name_list += "\n"
    name_list = name_list[:-1]
    developer_string = Developer_strings.developer_string6
    # prompt the model
    print("Prompting...")
    with open("secrets.json", "r") as file:
        secrets = json.load(file)
    client = OpenAI(
        organization=secrets["organization"],
        project=secrets["project"],
        api_key=secrets["api_key"],
    )
    answer = prompt(client, developer_string, name_list, 200)
    print(f"Developer:\n{developer_string}\nUser:\n{name_list}\nAnswer:\n{answer}")

    # check answers
    print("Checking...")
    answer_mask = ["True" in item for item in answer.split("\n")]
    correct_answers_mask = answer_mask and df["match"].to_list()
    print(answer_mask)
    print(df["match"].to_list())
    print(correct_answers_mask)


if __name__ == "__main__":
    test_on_dataset("datasets\zeroShot\proofOfConcept.csv")
    # test_single_prompt()
