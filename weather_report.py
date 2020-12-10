import aiohttp
import asyncio
import json
import matplotlib.pyplot as plt
import os
import re
import sqlite3

def is_k_space_system(system_name):
  return not re.match(r'J\d{6}', system_name) or re.match(r'AD.{3}', system_name) or re.match(r'P-\d{3}', system_name) or system_name == 'Thera'

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
  if not os.path.exists('jumps.json'):
    jumps = await get_system_jumps(session)
    with open('jumps.json', 'w') as file:
      json.dump(jumps, file)
  if not os.path.exists('killmails.json'):
    zkill_killmails = []
    for region_id in [region for region in await get_regions(session)]:
      for killmail in await get_zkill_regional_past_hour_kills(session, region_id):
        zkill_killmails.append(killmail)
    killmails = await asyncio.gather(*[get_killmail(session, zkill_killmail['killmail_id'], zkill_killmail['zkb']['hash']) for zkill_killmail in zkill_killmails])
    with open('killmails.json', 'w') as file:
      json.dump(killmails, file)
      

def store_data(cur, conn, row_limit):
  with open('systems.json', 'r') as systems_file, open('jumps.json', 'r') as jumps_file, open('killmails.json', 'r') as killmails_file, open('executions.txt', 'a+') as executions_file:
    systems = json.load(systems_file)
    jumps = json.load(jumps_file)
    killmails = json.load(killmails_file)
    current_row = 0
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
        current_row += 1
        if executions < 4 and current_row == row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
          return None
    for jump in jumps:
      if len(cur.execute('SELECT * FROM jumps WHERE id == ' + str(jump['system_id'])).fetchall()) == 0:
        cur.execute('INSERT INTO jumps (id, jumps) VALUES (?, ?)', (
          jump['system_id'],
          jump['ship_jumps']
        ))
        conn.commit()
        current_row += 1
        if executions < 4 and current_row == row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
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
        current_row += 1
        if executions < 4 and current_row == row_limit:
          executions_file.seek(0)
          if executions_file.read() == '':
            executions = 0
          else:
            executions_file.seek(0)
            executions = int(executions_file.read()[-1])
          executions_file.seek(0)
          executions_file.write(str(executions+1))
          executions_file.truncate()
          return None

def process_data(cur, conn):
  calculations_file = open('calculations.txt', 'w')

  highsec_kills = len(cur.execute('SELECT killmails.id, killmails.system_id, systems.name, systems.security_status FROM killmails INNER JOIN systems ON killmails.system_id=systems.id WHERE systems.security_status >= 0.5').fetchall())
  lowsec_kills = len(cur.execute('SELECT killmails.id, killmails.system_id, systems.name, systems.security_status FROM killmails INNER JOIN systems ON killmails.system_id=systems.id WHERE systems.security_status BETWEEN 0.1 AND 0.4').fetchall())
  nullsec_kills = len(cur.execute('SELECT killmails.id, killmails.system_id, systems.name, systems.security_status FROM killmails INNER JOIN systems ON killmails.system_id=systems.id WHERE systems.security_status <= 0.0').fetchall())
  
  calculations_file.write('Proportion of kills in New Eden in past hour from data collection time occurring in highsec: ' + str(highsec_kills/(highsec_kills+lowsec_kills+nullsec_kills)) + '\n')
  calculations_file.write('Proportion of kills in New Eden in past hour from data collection time occurring in lowsec ' + str(lowsec_kills/(highsec_kills+lowsec_kills+nullsec_kills)) + '\n')
  calculations_file.write('Proportion of kills in New Eden in past hour from data collection time occurring in nullsec: ' + str(nullsec_kills/(highsec_kills+lowsec_kills+nullsec_kills)) + '\n\n')

  plt.title('Proportion of Kills in New Eden in Past Hour from Data Collection Time by Security Classification of System')
  plt.pie([highsec_kills, lowsec_kills, nullsec_kills], labels=['Highsec', 'Lowsec', 'Nullsec'], autopct='%1.1f%%', startangle=90)
  plt.axis('equal')
  plt.show()

  highsec_jumps = 0
  for entry in cur.execute('SELECT jumps.id, jumps.jumps FROM jumps INNER JOIN systems ON jumps.id=systems.id WHERE systems.security_status >= 0.5').fetchall():
    highsec_jumps += entry[1]
  lowsec_jumps = 0
  for entry in cur.execute('SELECT jumps.id, jumps.jumps FROM jumps INNER JOIN systems ON jumps.id=systems.id WHERE systems.security_status BETWEEN 0.1 AND 0.4').fetchall():
    lowsec_jumps += entry[1]
  nullsec_jumps = 0
  for entry in cur.execute('SELECT jumps.id, jumps.jumps FROM jumps INNER JOIN systems ON jumps.id=systems.id WHERE systems.security_status <= 0.0').fetchall():
    nullsec_jumps += entry[1]

  calculations_file.write('Proportion of jumps in New Eden in past hour from data collection time occurring in highsec: ' + str(highsec_jumps/(highsec_jumps+lowsec_jumps+nullsec_jumps)) + '\n')
  calculations_file.write('Proportion of jumps in New Eden in past hour from data collection time occurring in lowsec: ' + str(lowsec_jumps/(highsec_jumps+lowsec_jumps+nullsec_jumps)) + '\n')
  calculations_file.write('Proportion of jumps in New Eden in past hour from data collection time occurring in nullsec: ' + str(nullsec_jumps/(highsec_jumps+lowsec_jumps+nullsec_jumps)) + '\n\n')

  plt.title('Proportion of Jumps in New Eden in Past Hour from Data Collection Time by Security Classification of System')
  plt.pie([highsec_jumps, lowsec_jumps, nullsec_jumps], labels=['Highsec', 'Lowsec', 'Nullsec'], autopct='%1.1f%%', startangle=90)
  plt.axis('equal')
  plt.show()

  systems = cur.execute('SELECT id FROM systems').fetchall()

  system_kills = {}
  for system in systems:
    kills = cur.execute('SELECT systems.id, systems.name, killmails.id FROM killmails INNER JOIN systems ON killmails.system_id=systems.id WHERE systems.id == ' + str(system[0])).fetchall()
    if len(kills) != 0 and kills[0][1][:2] != 'AD':
      system_kills[kills[0][1]] = len(kills)

  system_kills_explainer = 'Top 5 systems by kills in past hour from data collection time:\n'
  for entry in sorted(system_kills, key=system_kills.get, reverse=True)[:5]:
    system_kills_explainer += entry + ': ' + str(system_kills[entry]) + '\n'
  calculations_file.write(system_kills_explainer + '\n')

  top_5_systems_kills = sorted(system_kills, key=system_kills.get, reverse=True)[:5]
  top_5_systems_kills_values = [
    system_kills[top_5_systems_kills[0]],
    system_kills[top_5_systems_kills[1]],
    system_kills[top_5_systems_kills[2]],
    system_kills[top_5_systems_kills[3]],
    system_kills[top_5_systems_kills[4]]
  ]

  plt.bar(top_5_systems_kills, top_5_systems_kills_values)
  plt.title('Freqeuncy of Top 5 Systems By Kills in Past Hour From Data Collection Time')
  plt.xlabel('System Name')
  plt.ylabel('Frequency of Kills in Past Hour From Data Collection Time')
  plt.show()

  system_jumps = {}
  for system in systems:
    jumps = cur.execute('SELECT systems.id, systems.name, jumps.jumps FROM jumps INNER JOIN systems ON jumps.id=systems.id WHERE jumps.id == ' + str(system[0])).fetchall()
    if len(jumps) != 0 and jumps[0][1] != 'AD':
      system_jumps[jumps[0][1]] = jumps[0][2]

  system_jumps_explainer = 'Top 5 systems by jumps in past hour from data collection time:\n'
  for entry in sorted(system_jumps, key=system_jumps.get, reverse=True)[:5]:
    system_jumps_explainer += entry + ': ' + str(system_jumps[entry]) + '\n'
  calculations_file.write(system_jumps_explainer)

  top_5_systems_jumps = sorted(system_jumps, key=system_jumps.get, reverse=True)[:5]
  top_5_systems_jumps_values = [
    system_jumps[top_5_systems_jumps[0]],
    system_jumps[top_5_systems_jumps[1]],
    system_jumps[top_5_systems_jumps[2]],
    system_jumps[top_5_systems_jumps[3]],
    system_jumps[top_5_systems_jumps[4]]
  ]

  plt.bar(top_5_systems_jumps, top_5_systems_jumps_values)
  plt.title('Freqeuncy of Top 5 Systems By Jumps in Past Hour From Data Collection Time')
  plt.xlabel('System Name')
  plt.ylabel('Frequency of Jumps in Past Hour From Data Collection Time')
  plt.show()

  calculations_file.close()

async def main(loop):
  connector = aiohttp.TCPConnector(limit=50)
  async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
    await get_data(session)
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    store_data(cur, conn, row_limit = 25)
    process_data(cur, conn)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(loop))