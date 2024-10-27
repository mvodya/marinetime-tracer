import os
import psycopg2
import json
import pandas as pd
from tqdm import tqdm
import numpy as np
from datetime import datetime

conn = psycopg2.connect(f"dbname='{os.environ['POSTGRES_DB']}' user='{os.environ['POSTGRES_USER']}' host='{os.environ['POSTGRES_HOST']}' password='{os.environ['POSTGRES_PASSWORD']}'")

curs = conn.cursor()
curs.execute("SELECT version()")
print(curs.fetchone())
curs.close()

# Load data file
with open('data.json', 'r') as file:
    data = json.load(file)

# Read dataframe
df = pd.DataFrame.from_dict(data, orient='index')

# Prepare data
numeric_cols = ['LAT', 'LON', 'SPEED', 'COURSE', 'HEADING', 'ELAPSED', 'LENGTH', 'ROT', 'WIDTH',
                'L_FORE', 'W_LEFT', 'DWT', 'GT_SHIPTYPE']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
int_cols = ['SPEED', 'COURSE', 'HEADING', 'ELAPSED', 'LENGTH', 'ROT', 'WIDTH', 'L_FORE', 'W_LEFT', 'DWT', 'GT_SHIPTYPE']
df[int_cols] = df[int_cols].astype('Int64')
df.replace({np.nan: None}, inplace=True)
df['FLAG'] = df['FLAG'].replace('--', None)
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='s')


# Add missing flags
try:
  with conn.cursor() as cursor:
    cursor.execute("SELECT flag FROM flags")
    existing_flags = [row[0] for row in cursor.fetchall()]
    existing_flags
    missing_flags = [{'flag': flag} for flag in df['FLAG'].unique() if flag not in existing_flags and flag is not None]

    insert_query = """
      INSERT INTO flags (flag)
      VALUES (%(flag)s)
    """
    cursor.executemany(insert_query, missing_flags)
    conn.commit()
    print(f"Added {len(missing_flags)} flags ({len(existing_flags)} already existing)")
    cursor.execute("SELECT id, flag FROM flags")
    # Load flags DB
    flags = {}
    for f in cursor.fetchall():
      flags[f[1]] = f[0]
except Exception as e:
  conn.rollback()
  print(f"Error processing: {e}")


# Add missing destinations
try:
  with conn.cursor() as cursor:
    cursor.execute("SELECT name FROM destinations")
    existing_destinations = [row[0] for row in cursor.fetchall()]
    existing_destinations
    missing_destinations = [{'name': dest} for dest in df['DESTINATION'].unique() if dest not in existing_destinations and dest is not None]

    insert_query = """
      INSERT INTO destinations (name)
      VALUES (%(name)s)
    """
    cursor.executemany(insert_query, missing_destinations)
    conn.commit()
    print(f"Added {len(missing_destinations)} destinations ({len(existing_destinations)} already existing)")
except Exception as e:
  conn.rollback()
  print(f"Error processing: {e}")

description = os.environ.get('DESCRIPTION', 'UNKNOWN')

# Register parse event
try:
  with conn.cursor() as cursor:
    insert_query = """
      INSERT INTO parses (start, "end", description)
      VALUES (%(start)s, %(end)s, %(description)s)
      RETURNING id
    """
    d = {
        'start': df['TIMESTAMP'].min(),
        'end': df['TIMESTAMP'].max(),
        'description': f'{description}',
    }
    cursor.execute(insert_query, d)
    parses_id = cursor.fetchone()[0]
    conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error processing: {e}")


# Add missing ships and record new ships positions
try:
  for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    with conn.cursor() as cursor:
      cursor.execute("SELECT id FROM ships WHERE ship_id = %s LIMIT 1", (index,))
      ship = cursor.fetchone()
      if not ship:
        insert_query = """
          INSERT INTO ships (ship_id, name, flag_id, width, l_fore, w_left, length)
          VALUES (%(ship_id)s, %(shipname)s, %(flag_id)s, %(width)s, %(l_fore)s, %(w_left)s, %(length)s)
          RETURNING id
        """
        d = {
            'ship_id': row['SHIP_ID'],
            'shipname': row['SHIPNAME'],
            'flag_id': flags[row['FLAG']] if row['FLAG'] is not None else None,
            'width': row['WIDTH'],
            'l_fore': row['L_FORE'],
            'w_left': row['W_LEFT'],
            'length': row['LENGTH']
        }
        cursor.execute(insert_query, d)
        ship_id = cursor.fetchone()[0]
        conn.commit()
      else:
        ship_id = ship[0]
      insert_query = """
          INSERT INTO positions (ship_id, timestamp, location, speed, course, heading, rot, dwt, type, gt_type, parse_id, destination)
          VALUES (%(ship_id)s, %(timestamp)s, %(location)s, %(speed)s, %(course)s, %(heading)s, %(rot)s, %(dwt)s, %(type)s, %(gt_type)s, %(parse_id)s, %(destination)s)
        """
      d = {
          'ship_id': ship_id,
          'timestamp': row['TIMESTAMP'],
          'location': f"({row['LAT']}, {row['LON']})",
          'speed': row['SPEED'],
          'course': row['COURSE'],
          'heading': row['HEADING'],
          'rot': row['ROT'],
          'dwt': row['DWT'],
          'type': row['SHIPTYPE'],
          'gt_type': row['GT_SHIPTYPE'],
          'parse_id': parses_id,
          'destination': row['DESTINATION']
      }
      cursor.execute(insert_query, d)
      conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error processing: {e}")