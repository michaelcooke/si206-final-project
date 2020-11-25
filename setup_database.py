import aiohttp
import asyncio
import os
import sqlite3

def create_jumps_table(cur, conn):
  print('Adding jumps table')
  cur.execute('DROP TABLE IF EXISTS jumps')
  cur.execute('CREATE TABLE jumps (id INTEGER PRIMARY KEY, jumps INTEGER)')
  conn.commit()

def create_killmails_table(cur, conn):
  print('Adding killmails table')
  cur.execute('DROP TABLE IF EXISTS killmails')
  cur.execute('CREATE TABLE killmails (id INTEGER PRIMARY KEY, system_id INTEGER, war_kill BOOLEAN, killmail_time DATETIME)')
  conn.commit()

async def get_regions(session):
  try:
    async with session.get('https://esi.evetech.net/latest/universe/regions/?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_regions(session)

async def get_region(session, region_id):
  try:
    async with session.get('https://esi.evetech.net/latest/universe/regions/' + str(region_id) + '/?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_region(session, region_id)

async def seed_regions(session, cur, conn):
  print('Seeding regions')
  region_ids = await get_regions(session)
  regions = await asyncio.gather(*[get_region(session, region_id) for region_id in region_ids])

  cur.execute('DROP TABLE IF EXISTS regions')
  cur.execute('CREATE TABLE regions (id INTEGER PRIMARY KEY, name VARCHAR(255))')

  for region in regions:
    cur.execute('INSERT INTO regions (id, name) VALUES (?, ?)', (
      region['region_id'],
      region['name']
    ))

  conn.commit()

async def get_systems(session):
  try:
    async with session.get('https://esi.evetech.net/latest/universe/systems/?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_systems(session)

async def get_system(session, system_id):
  try:
    async with session.get('https://esi.evetech.net/latest/universe/systems/' + str(system_id) + '/?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_system(session, system_id)

async def seed_systems(session, cur, conn):
  print('Seeding systems')
  system_ids = await get_systems(session)
  systems = await asyncio.gather(*[get_system(session, system_id) for system_id in system_ids])

  cur.execute('DROP TABLE IF EXISTS systems')
  cur.execute('CREATE TABLE systems (id INTEGER PRIMARY KEY, name VARCHAR(255), security_status REAL)')

  for system in systems:
    cur.execute('INSERT INTO systems (id, name, security_status) VALUES (?, ?, ?)', (
      system['system_id'],
      system['name'],
      round(float(system['security_status']), 1),
      system
    ))

  conn.commit()

def create_database(name):
  print('Creating ' + name)
  path = os.path.dirname(os.path.abspath(__file__))
  conn = sqlite3.connect(path + '/' + name)
  cur  = conn.cursor()
  return cur, conn

async def main(loop):
  cur, conn = create_database('database.db')
  async with aiohttp.ClientSession(loop=loop) as session:
    await seed_regions(session, cur, conn)
    await seed_systems(session, cur, conn)
    create_jumps_table(cur, conn)
    create_killmails_table(cur, conn)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(loop))
