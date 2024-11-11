import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    dynamodb_resource = boto3.resource('dynamodb')
    dynamodb_table = dynamodb_resource.Table('TablaWebScrappingPokemon')

    url = "https://pokemondb.net/pokedex/all"
    response = requests.get(url)
    if response.status_code != 200:
        return {'statusCode': response.status_code, 'body': 'Error accessing the webpage'}

    soup = BeautifulSoup(response.content, 'html.parser')
    html_table = soup.find('table', id='pokedex')
    if not html_table:
        return {'statusCode': 404, 'body': 'No table found on the webpage'}

    rows = []
    count = 0

    for row in html_table.find('tbody').find_all('tr'):
        if count >= 50:
            break
        cells = row.find_all('td')
        if len(cells) >= 10:
            pokedex_number = cells[0].find('span').text.strip() if cells[0].find('span') else 'N/A'
            name = cells[1].find('a', class_='ent-name').text.strip() if cells[1].find('a', class_='ent-name') else 'N/A'
            types = [type_elem.text.strip() for type_elem in cells[2].find_all('a')]
            total = cells[3].text.strip()
            hp = cells[4].text.strip()
            attack = cells[5].text.strip()
            defense = cells[6].text.strip()
            sp_atk = cells[7].text.strip()
            sp_def = cells[8].text.strip()
            speed = cells[9].text.strip()

            rows.append({
                'PokedexNumber': pokedex_number,
                'Name': name,
                'Type': ", ".join(types),
                'Total': total,
                'HP': hp,
                'Attack': attack,
                'Defense': defense,
                'Sp. Atk': sp_atk,
                'Sp. Def': sp_def,
                'Speed': speed
            })
            count += 1

    scan = dynamodb_table.scan()
    with dynamodb_table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})
            
    for i, row in enumerate(rows, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        dynamodb_table.put_item(Item=row)

    return {'statusCode': 200, 'body': rows}
