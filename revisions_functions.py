import json 
import requests
from tqdm import tqdm
import os


def read_json_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f]
    
def get_revision_ids(jsonlist):
    return [obj['id'] for obj in jsonlist]

def compare_revisions(rev1, rev2):
    url = f'https://api.wikimedia.org/core/v1/wikipedia/en/revision/{rev2}/compare/{rev1}'
    headers = {
        'User-Agent': 'WikiSandbox/ManagingBigData'  # Replace with your project and email
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    return data

def save_revision_content_to_json(data, rev1, rev2, title, date):
    if 'diff' not in data.keys():
        print(f"Error: No diff found for revisions {rev1} and {rev2}")
    else:
        for change in data['diff']:
            change['from_id'] = rev1
            change['to_id'] = rev2
            change['title'] = title
            change['date'] = date
            with open(f'rev_data/{title}_revision_content.json', 'a', encoding='utf-8') as f:
                json.dump(change, f, ensure_ascii=False)
                f.write('\n')

def process_revisions(jsonlist, idList, title):
    for i in tqdm(range(1, len(idList))):
        date = jsonlist[i]['timestamp']
        data = compare_revisions(str(idList[i-1]), str(idList[i]))
        save_revision_content_to_json(data, str(idList[i-1]), str(idList[i]), title, date)

def save_revisions_to_hdfs(revisions, filename):
    """
    Saves the revisions in an HDFS-compatible format (JSON Lines).
    """

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "a", encoding="utf-8") as f:
        for revision in revisions:
            json.dump(revision, f, ensure_ascii=False)
            f.write("\n")  # Each revision is a separate JSON line


def get_article_revisions(title, older_than=None):
    """
    Fetches revisions for a given article.
    """
    url = f'https://api.wikimedia.org/core/v1/wikipedia/en/page/{title}/history'
    parameters = {}
    if older_than:
        parameters['older_than'] = older_than
    headers = {
        'User-Agent': 'WikiSandbox/ManagingBigData'  # Replace with your project and email
    }
    response = requests.get(url, headers=headers, params=parameters)
    if response.status_code != 200:
        if response.status_code == 429:
            print(response.json())  
            print("Rate limit exceeded. Please wait and try again later.")
            return "429"
        print(f"Error fetching revisions for {title}: {response.status_code}")
    else:
        data = response.json()
        return data.get("revisions", [])

def loop_through_revisions(title, from_date=None, olderThanId=None):
    """
    Loops through revisions for an article until a specific timestamp is reached.
    """
    revisions = []
    while True:
        new_revisions = get_article_revisions(title, older_than=olderThanId)
        if new_revisions == "429":
            return "429"
        if not new_revisions:
            break
        revisions.extend(new_revisions)
        if new_revisions[-1]["timestamp"] <= from_date:
            break
        olderThanId = new_revisions[-1]["id"]
    print(f"Gathered {len(revisions)} revisions for article: {title}")
    return revisions

def get_revData_and_revContent(title, from_date):
    revisions = loop_through_revisions(title, from_date)
    if revisions == "429":
        return
    if not revisions:
        print(f"No revisions found for article: {title}")
        remove_title_from_json(title)
        return
    save_revisions_to_hdfs(revisions, f'rev_data/{title}_revisions.json')
    idList = get_revision_ids(revisions)
    process_revisions(revisions, idList, title)
    remove_title_from_json(title)
    print(f"Finished processing revisions for article: {title}")

revisions_from = "2020-04-30T13:16:57Z"


def remove_title_from_json(title):
    with open(f'titlesUkr.json', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    with open(f'titlesUkr.json', 'w', encoding='utf-8') as f:
        for line in lines:
            if title not in line:
                f.write(line)
#read titles json 
with open('titlesUkr.json', "r", encoding="utf-8") as f:
    titles = json.load(f)


for title in tqdm(titles):
    get_revData_and_revContent(title, revisions_from)