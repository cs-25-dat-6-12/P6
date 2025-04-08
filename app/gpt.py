from openai import OpenAI
import json

test_name_LASKI = "{\"given-name\": \"Zigmund\", \"surname\": \"Ehrlich\"}"
test_name_Zylbercweig = "{\"given-name\": \"Zygmund\", \"surname\": \"Ehriykh\"}"

class Developer_strings():
    developer_string1 = "You are a language model that does entity resolution, You will be given two names split up into name parts, one from the Zylbercweig dataset and one from the LASKI dataset. The Zylbercweig dataset contains names written in the hebrew alphabet which have been transliterated into the roman alphabet, while the LASKI dataset contains names written in the roman alphabet. Your task is to determine whether the two names refer to the same person. Reply only with \"True\" or \"False\""


def prompt(client, developer_prompt, user_prompt, tokens=200):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "developer",
            "content": [{
                "type": "text",
                "text": developer_prompt
            }]},
            {"role": "user",
            "content": [{
                "type": "text",
                "text": user_prompt
            }]},
        ],
        max_tokens = tokens
    )
    return completion.choices[0].message.content

def string_from_name_parts(name_parts):
    return_string = "{"
    for entry in name_parts:
        return_string += entry
        return_string += ": "
        return_string += name_parts[entry]
        return_string += ", "
    return_string = return_string[0:len(return_string)-2]
    return_string += "}"
    return return_string

def create_prompt_string(Zylbercweig_name_parts, LASKI_name_parts):
    return "Do these two names refer to the same person?\n" + "Name from the Zylbercweig dataset: " + string_from_name_parts(Zylbercweig_name_parts) + "\nName from the LASKI dataset: " + string_from_name_parts(LASKI_name_parts)

if __name__ == "__main__":
    with open('secrets.json', 'r') as file:
        secrets = json.load(file)
    client = OpenAI(
        organization = secrets["organization"],
        project = secrets["project"],
        api_key = secrets["api_key"],
        )
    prompt_string = create_prompt_string(json.loads(test_name_Zylbercweig), json.loads(test_name_LASKI))
    answer = prompt(client, Developer_strings.developer_string1, prompt_string, 20)
    print(answer)