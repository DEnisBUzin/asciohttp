import asyncpg
import asyncio

DB_URL = "postgresql://username:password@localhost/starwars"

async def create_table():
    conn = await asyncpg.connect(DB_URL)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        birth_year TEXT,
        eye_color TEXT,
        films TEXT,
        gender TEXT,
        hair_color TEXT,
        height TEXT,
        homeworld TEXT,
        mass TEXT,
        skin_color TEXT,
        species TEXT,
        starships TEXT,
        vehicles TEXT
    );
    """)
    await conn.close()

if __name__ == "__main__":
    asyncio.run(create_table())