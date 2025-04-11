from openai import OpenAI
import json

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


if __name__ == "__main__":
    developer_string = Developer_strings.developer_string5
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
    answer = prompt(client, developer_string, prompt_string, 200)
    # answer = prompt(client, developer_string, test_name_list, 200)
    print(f"Developer:\n{developer_string}\nUser:\n{prompt_string}\nAnswer:\n{answer}")
