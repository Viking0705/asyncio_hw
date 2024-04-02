import asyncio
import datetime
import aiohttp

from more_itertools import chunked

from models import Session, SwapiPeople, close_db, init_db

CHUNK_SIZE = 20


async def insert_people(people_list):
    people_list = [SwapiPeople(**person) for person in people_list if person.get('detail') != 'Not found']
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_person(person_id):
    session = aiohttp.ClientSession()    
    response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_response = await response.json()
    link_fields = ['films', 'species', 'starships', 'vehicles']
    if json_response.get('detail') != 'Not found':
        data_dict = dict()
        for key in SwapiPeople.__table__.columns.keys():            
            if key != 'id':
                if key in link_fields:
                    coros = [get_name_link(link, key) for link in json_response.get(key)]
                    data_dict.setdefault(key, ', '.join(await asyncio.gather(*coros)))
                else:
                    data_dict.setdefault(key, json_response.get(key))
            else:
                data_dict.setdefault('id', person_id)
        res = data_dict
    else:
        res = json_response
    await session.close()
    return res


async def get_name_link(link, field):
    session = aiohttp.ClientSession()
    response = await session.get(link)
    json_response = await response.json()
    await session.close()
    if field == 'films':
        return json_response['title']
    else:
        return json_response['name']


async def main():
    await init_db()

    for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id) for person_id in person_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_people(result))
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks)
    await close_db()


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
