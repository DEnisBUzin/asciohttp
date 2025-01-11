import aiohttp
import asyncio
from databases import Database

DB_URL = "postgresql://username:password@localhost/starwars"
database = Database(DB_URL)


async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()


async def process_character(character_url):
    data = await fetch_data(character_url)

    films = ", ".join([await fetch_data(film_url)["title"] for film_url in data["films"]])
    species = ", ".join([await fetch_data(species_url)["name"] for species_url in data["species"]])
    starships = ", ".join([await fetch_data(ship_url)["name"] for ship_url in data["starships"]])
    vehicles = ", ".join([await fetch_data(vehicle_url)["name"] for vehicle_url in data["vehicles"]])
    homeworld = await fetch_data(data["homeworld"])["name"] if data["homeworld"] else None

    record = {
        "id": int(data["url"].split("/")[-2]),
        "name": data["name"],
        "birth_year": data["birth_year"],
        "eye_color": data["eye_color"],
        "films": films,
        "gender": data["gender"],
        "hair_color": data["hair_color"],
        "height": data["height"],
        "homeworld": homeworld,
        "mass": data["mass"],
        "skin_color": data["skin_color"],
        "species": species,
        "starships": starships,
        "vehicles": vehicles
    }

    query = """
    INSERT INTO characters (id, name, birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, skin_color, species, starships, vehicles)
    VALUES (:id, :name, :birth_year, :eye_color, :films, :gender, :hair_color, :height, :homeworld, :mass, :skin_color, :species, :starships, :vehicles)
    ON CONFLICT (id) DO NOTHING;
    """
    await database.execute(query, record)


async def main():
    await database.connect()

    next_url = "https://swapi.dev/api/people/"
    while next_url:
        page_data = await fetch_data(next_url)
        tasks = [process_character(char_url) for char_url in [char["url"] for char in page_data["results"]]]
        await asyncio.gather(*tasks)
        next_url = page_data.get("next")

    await database.disconnect()


if __name__ == "__main__":
    asyncio.run(main())