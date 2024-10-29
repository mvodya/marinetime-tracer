import os
import psycopg2
import json
import pandas as pd
from tqdm import tqdm
import numpy as np
from datetime import datetime
from tqdm import tqdm
import psycopg2.extras

# Size of batches for DB inserting
BATCH_SIZE = 500

conn = psycopg2.connect(
    f"dbname='{os.environ['POSTGRES_DB']}' user='{os.environ['POSTGRES_USER']}' host='{os.environ['POSTGRES_HOST']}' port='{os.environ['POSTGRES_PORT']}' password='{os.environ['POSTGRES_PASSWORD']}'")

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
int_cols = ['SPEED', 'COURSE', 'HEADING', 'ELAPSED', 'LENGTH',
            'ROT', 'WIDTH', 'L_FORE', 'W_LEFT', 'DWT', 'GT_SHIPTYPE']
df[int_cols] = df[int_cols].astype('Int64')
df.replace({np.nan: None}, inplace=True)
df['FLAG'] = df['FLAG'].replace('--', None)
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='s')

# Add missing flags
try:
    with conn.cursor() as cursor:
        # Fetch existing flags
        cursor.execute("SELECT flag FROM flags")
        existing_flags = {row[0] for row in cursor.fetchall()}

        # Identify missing flags
        missing_flags = [{'flag': flag} for flag in df['FLAG'].unique(
        ) if flag not in existing_flags and flag is not None]

        # Insert missing flags in batches
        if missing_flags:
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO flags (flag)
                VALUES (%(flag)s)
            """, missing_flags)
            conn.commit()
            print(f"Added {len(missing_flags)} flags")

        # Reload flags with their IDs
        cursor.execute("SELECT id, flag FROM flags")
        flags = {row[1]: row[0] for row in cursor.fetchall()}

except Exception as e:
    conn.rollback()
    print("Error processing flags")
    raise e

# Add missing destinations
try:
    with conn.cursor() as cursor:
        # Fetch existing destinations
        cursor.execute("SELECT name FROM destinations")
        existing_destinations = {row[0] for row in cursor.fetchall()}

        # Identify missing destinations
        missing_destinations = [{'name': dest} for dest in df['DESTINATION'].unique(
        ) if dest not in existing_destinations and dest is not None]

        # Insert missing destinations in batches
        if missing_destinations:
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO destinations (name)
                VALUES (%(name)s)
            """, missing_destinations)
            conn.commit()
            print(f"Added {len(missing_destinations)} destinations")

except Exception as e:
    conn.rollback()
    print("Error processing destinations")
    raise e

description = os.environ.get('DESCRIPTION', 'UNKNOWN')

# Register parse event
try:
    with conn.cursor() as cursor:
        insert_query = """
            INSERT INTO parses (parse_start, parse_end, description)
            VALUES (%(parse_start)s, %(parse_end)s, %(description)s)
            RETURNING id
        """
        d = {
            'parse_start': df['TIMESTAMP'].min(),
            'parse_end': df['TIMESTAMP'].max(),
            'description': f'{description}',
        }
        cursor.execute(insert_query, d)
        parses_id = cursor.fetchone()[0]
        conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error processing parse event: {e}")

# Preload all existing ship_id with their internal ids
with conn.cursor() as cursor:
    cursor.execute("SELECT ship_id, id FROM ships")
    existing_ships = {row[0]: row[1]
                      for row in cursor.fetchall()}  # dictionary {ship_id: id}

try:
    for i in tqdm(range(0, len(df), BATCH_SIZE), desc="Processing batches"):
        batch = df.iloc[i:i + BATCH_SIZE]

        # Prepare data for inserting new ships
        new_ships = []
        positions = []

        for _, row in batch.iterrows():
            # Check if the ship exists
            ship_id = existing_ships.get(row['SHIP_ID'])

            if ship_id is None:
                # If the ship is new, prepare it for insertion
                new_ships.append({
                    'ship_id': row['SHIP_ID'],
                    'shipname': row['SHIPNAME'],
                    'flag_id': flags.get(row['FLAG'], None),
                    'width': row['WIDTH'],
                    'l_fore': row['L_FORE'],
                    'w_left': row['W_LEFT'],
                    'length': row['LENGTH']
                })
            else:
                # If the ship already exists, use its ID for the position
                positions.append({
                    'ship_id': ship_id,
                    'parsed_date': row['TIMESTAMP'],
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
                })

        with conn.cursor() as cursor:
            # Insert new ships and retrieve their ids
            if new_ships:
                psycopg2.extras.execute_batch(cursor, """
                    INSERT INTO ships (ship_id, name, flag_id, width, l_fore, w_left, length)
                    VALUES (%(ship_id)s, %(shipname)s, %(flag_id)s, %(width)s, %(l_fore)s, %(w_left)s, %(length)s)
                    RETURNING ship_id, id
                """, new_ships)
                cursor.execute("SELECT ship_id, id FROM ships")

                # Add new ship_id to the dictionary of existing ships with their ids
                for ship_id, db_id in cursor.fetchall():
                    existing_ships[ship_id] = db_id

            # Insert all positions into the positions table
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO positions (ship_id, parsed_date, location, speed, course, heading, rot, dwt, type, gt_type, parse_id, destination)
                VALUES (%(ship_id)s, %(parsed_date)s, %(location)s, %(speed)s, %(course)s, %(heading)s, %(rot)s, %(dwt)s, %(type)s, %(gt_type)s, %(parse_id)s, %(destination)s)
            """, positions)

            # Commit transaction after each batch
            conn.commit()

except Exception as e:
    conn.rollback()
    print("Error processing positions")
    raise e
