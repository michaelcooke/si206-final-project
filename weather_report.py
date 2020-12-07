import aiohttp
import asyncio
import json
import os
import re
import sqlite3

def is_k_space_system(system_name):
  return not re.match(r'J\d{6}', system_name) or re.match(r'AD\d{3}', system_name) or re.match(r'P-\d{3}', system_name) or system_name == 'Thera'

async def get_killmail(session, killmail_id, killmail_hash):
  try:
    async with session.get('https://esi.evetech.net/latest/killmails/' + str(killmail_id) + '/' + str(killmail_hash) + '?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_killmail(session, killmail_id, killmail_hash)

async def get_zkill_regional_past_hour_kills(session, region_id):
  url = 'https://zkillboard.com/api/regionID/' + str(region_id) + '/pastSeconds/' + str(3600) + '/'
  try:
    async with session.get(url) as response:
      return await response.json(content_type='application/json')
  except:
    return await get_zkill_regional_past_hour_kills(session, region_id)

async def get_system_jumps(session):
  try:
    async with session.get('https://esi.evetech.net/latest/universe/system_jumps?datasource=tranquility&language=en-us') as response:
      return await response.json(content_type='application/json')
  except:
    return await get_system_jumps(session)

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

async def get_data(session):
  if not os.path.exists('systems.json'):
    systems = await asyncio.gather(*[get_system(session, system_id) for system_id in await get_systems(session)])
    with open('systems.json', 'w') as file:
      json.dump(systems, file)
      file.close()
  if not os.path.exists('jumps.json'):
    jumps = await get_system_jumps(session)
    with open('jumps.json', 'w') as file:
      json.dump(jumps, file)
      file.close()
  if not os.path.exists('killmails.json'):
    zkill_killmails = []
    for region_id in [region for region in await get_regions(session)]:
      for killmail in await get_zkill_regional_past_hour_kills(session, region_id):
        zkill_killmails.append(killmail)
    killmails = await asyncio.gather(*[get_killmail(session, zkill_killmail['killmail_id'], zkill_killmail['zkb']['hash']) for zkill_killmail in zkill_killmails])
    with open('killmails.json', 'w') as file:
      json.dump(killmails, file)
      file.close()

def store_data(cur, conn, silly_asinine_arbitrary_row_limit):
  with open('systems.json', 'r') as systems_file, open('jumps.json', 'r') as jumps_file, open('killmails.json', 'r') as killmails_file, open('executions.txt', 'a+') as executions_file:
    systems = json.load(systems_file)
    systems_file.close()
    jumps = json.load(jumps_file)
    jumps_file.close()
    killmails = json.load(killmails_file)
    killmails_file.close()
    current_silly_asinine_arbitrary_row = 0
    executions_file.seek(0)
    if executions_file.read() == '':
      executions = 0
    else:
      executions_file.seek(0)
      executions = int(executions_file.read()[-1])
    executions_file.seek(0)
    if executions == 0:
      cur.execute('CREATE TABLE IF NOT EXISTS systems (id INTEGER PRIMARY KEY, name VARCHAR(255), security_status REAL)')
      cur.execute('CREATE TABLE IF NOT EXISTS jumps (id INTEGER PRIMARY KEY, jumps INTEGER)')
      cur.execute('CREATE TABLE IF NOT EXISTS killmails (id INTEGER PRIMARY KEY, system_id INTEGER, war_kill BOOLEAN, killmail_time DATETIME)')
    for system in systems:
      if is_k_space_system(system['name']) and len(cur.execute('SELECT * FROM systems WHERE id == ' + str(system['system_id'])).fetchall()) == 0:
        cur.execute('INSERT INTO systems (id, name, security_status) VALUES (?, ?, ?)', (
          system['system_id'],
          system['name'],
          round(float(system['security_status']), 1)
        ))
        conn.commit()
        current_silly_asinine_arbitrary_row += 1
        if executions < 4 and current_silly_asinine_arbitrary_row == silly_asinine_arbitrary_row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
          executions_file.close()
          return None
    for jump in jumps:
      if len(cur.execute('SELECT * FROM jumps WHERE id == ' + str(jump['system_id'])).fetchall()) == 0:
        cur.execute('INSERT INTO jumps (id, jumps) VALUES (?, ?)', (
          jump['system_id'],
          jump['ship_jumps']
        ))
        conn.commit()
        current_silly_asinine_arbitrary_row += 1
        if executions < 4 and current_silly_asinine_arbitrary_row == silly_asinine_arbitrary_row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
          executions_file.close()
          return None
    for killmail in killmails:
      if is_k_space_system(system['name']) and len(cur.execute('SELECT * FROM killmails WHERE id == ' + str(killmail['killmail_id'])).fetchall()) == 0:
        cur.execute('INSERT INTO killmails (id, system_id, war_kill, killmail_time) VALUES (?, ?, ?, ?)', (
          killmail['killmail_id'],
          killmail['solar_system_id'],
          True if 'war_id' in killmail else False,
          killmail['killmail_time']
        ))
        conn.commit()
        current_silly_asinine_arbitrary_row += 1
        if executions < 4 and current_silly_asinine_arbitrary_row == silly_asinine_arbitrary_row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
          executions_file.close()
          return None

def process_data(cur, conn):
  pass

async def main(loop):
  connector = aiohttp.TCPConnector(limit=50)
  async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
    await get_data(session)
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    store_data(cur, conn, silly_asinine_arbitrary_row_limit = 25)
    process_data(cur, conn)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(loop))