import asyncio
import aiohttp
from more_itertools import chunked
import requests
from models import init_db, SwapiPeople, Session, engine

MAX_CHUNK = 10
count_person = requests.get('https://swapi.dev/api/people/').json()['count']


async def get_person(client, person_id):
    films_list = []
    species_list = []
    starships_list = []
    vehicles_list = []
    json_result = {}
    http_response = await client.get(f'https://swapi.dev/api/people/{person_id}/')
    if http_response.status == 200:
        json_result = await http_response.json()
        json_result.pop('created')
        json_result.pop('edited')
        json_result.pop('url')
        homeworld_response = await client.get(json_result['homeworld'])
        homeworld_result = await homeworld_response.json()
        json_result['homeworld'] = homeworld_result['name']
        for film_link in json_result['films']:
            film_response = await client.get(film_link)
            film_result = await film_response.json()
            films_list.append(film_result['title'])
        json_result['films'] = ', '.join(films_list)
        for species_link in json_result['species']:
            species_response = await client.get(species_link)
            species_result = await species_response.json()
            species_list.append(species_result['name'])
        json_result['species'] = ', '.join(species_list)
        for starship_link in json_result['starships']:
            starship_response = await client.get(starship_link)
            starship_result = await starship_response.json()
            starships_list.append(starship_result['name'])
        json_result['starships'] = ', '.join(starships_list)
        for vehicle_link in json_result['vehicles']:
            vehicle_response = await client.get(vehicle_link)
            vehicle_result = await vehicle_response.json()
            vehicles_list.append(vehicle_result['name'])
        json_result['vehicles'] = ', '.join(starships_list)
    return json_result


async def insert_to_db(list_of_jsons):
    models = [SwapiPeople(**json_result) for json_result in list_of_jsons]
    async with Session() as session:
        session.add_all(models)
        await session.commit()


async def main():
    await init_db()
    client = aiohttp.ClientSession()
    for chunk in chunked(range(1, count_person + 1), MAX_CHUNK):
        coros = [get_person(client, person_id) for person_id in chunk]
        result = await asyncio.gather(*coros)
        await asyncio.create_task(insert_to_db(result))
        # print(result)
    tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks_set)
    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
