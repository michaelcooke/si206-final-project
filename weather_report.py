import aiohttp
import asyncio
import os
import re
import sqlite3

def is_k_space_system(system_name, cur, conn):
  if system_name not in get_system_names(cur, conn) or re.match(r'J\d{6}', system_name) or re.match(r'AD\d{3}', system_name) or re.match(r'P-\d{3}', system_name) or system_name == 'Thera':
    return False
  return True

def get_k_space_system_ids(cur, conn):
  k_space_system_ids = []

  results = cur.execute('SELECT id FROM systems').fetchall()
  for i in range(len(results)):
    if is_k_space_system(results[i][0], cur, conn):
      k_space_system_ids.append(results[i][0])

  return k_space_system_ids

def get_region_ids(cur, conn):
  results = cur.execute('SELECT id FROM regions').fetchall()
  return [results[i][0] for i in range(len(results))]

def get_constellation_ids(cur, conn):
  results = cur.execute('SELECT id FROM constellations').fetchall()
  return [results[i][0] for i in range(len(results))]

def get_system_names(cur, conn):
  results = cur.execute('SELECT name FROM systems').fetchall()
  return [results[i][0] for i in range(len(results))]

def get_system_ids(cur, conn):
  results = cur.execute('SELECT id FROM systems').fetchall()
  return [results[i][0] for i in range(len(results))]

def get_system_name(system_id, cur, conn):
  return cur.execute('SELECT name FROM systems WHERE id = ?', [system_id]).fetchall()[0][0]

async def get_system_jumps(session):
  url = 'https://esi.evetech.net/latest/universe/system_jumps?datasource=tranquility&language=en-us'
  try:
    async with session.get(url) as response:
      if response.status == 200:
        return await response.json(content_type='application/json')
      return await get_system_jumps(session)
  except:
    return await get_system_jumps(session)

async def get_killmail(session, killmail_id, killmail_hash):
  print('Getting killmail id/hash: ' + str(killmail_id) + '/' + str(killmail_hash))
  url = 'https://esi.evetech.net/latest/killmails/' + str(killmail_id) + '/' + str(killmail_hash) + '?datasource=tranquility&language=en-us'
  try:
    async with session.get(url) as response:
      if response.status == 200:
        return await response.json(content_type='application/json')
      return await get_killmail(session, killmail_id, killmail_hash)
  except:
    return await get_killmail(session, killmail_id, killmail_hash)

async def get_region_kills_past_seconds_ago(session, region_id, seconds):
  print('Getting kills from region id: ' + str(region_id))
  url = 'https://zkillboard.com/api/regionID/' + str(region_id) + '/pastSeconds/' + str(seconds) + '/'
  try:
    async with session.get(url) as response:
      return await response.json(content_type='application/json')
  except:
    return await get_region_kills_past_seconds_ago(session, region_id, seconds)

async def seed_killmails(session, seconds, cur, conn):
  print('Dropping old killmails')
  cur.execute('DELETE FROM killmails')
  conn.commit()

  print('Seeding killmails from past ' + str(seconds) + ' seconds')

  zkill_killmails = []
  for region_id in get_region_ids(cur, conn):
    for killmail in await get_region_kills_past_seconds_ago(session, region_id, seconds):
      zkill_killmails.append(killmail)

  killmails = await asyncio.gather(*[get_killmail(session, zkill_killmail['killmail_id'], zkill_killmail['zkb']['hash']) for zkill_killmail in zkill_killmails])

  for entry in await get_system_jumps(session):
    cur.execute('INSERT INTO jumps (id, jumps) VALUES (?, ?)', (
      entry['system_id'],
      entry['ship_jumps']
    ))
    conn.commit()

  for killmail in killmails:
    cur.execute('INSERT INTO killmails (id, system_id, war_kill, killmail_time) VALUES (?, ?, ?, ?)', (
      killmail['killmail_id'],
      killmail['solar_system_id'],
      True if 'war_id' in killmail else False,
      killmail['killmail_time']
    ))
    conn.commit()

async def main(loop):
  connector = aiohttp.TCPConnector(limit=50)
  async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
    name = 'database.db'
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + name)
    cur  = conn.cursor()
    await seed_killmails(session, 60*60, cur, conn)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(loop))
