from seleniumbase import Driver

import json
import re
from tqdm import tqdm

def mtRunner(positions):
    output = []
    print("Open webdriver")
    driver = Driver(uc=True, uc_cdp_events=True, headless=True)
    raw = []
    # Move to position

    def moveTo(pos):
        driver.execute_script(
            f"mtMap.setView({{'lon': {pos['lon']}, 'lat': {pos['lat']}}}, {pos['zoom']});")
    # Handle traffic data

    def save(data):
        req = {}
        req["id"] = data["params"]["requestId"]
        req["path"] = data["params"]["headers"][":path"]
        if "/getData/" in req["path"]:
            raw.append(req)
    # Listen traffic
    driver.add_cdp_listener('Network.requestWillBeSentExtraInfo', save)

    print("Open marine traffic")
    # Open marine traffic
    driver.open(
        "https://www.marinetraffic.com/en/ais/home/centerx:132.2/centery:43.0/zoom:10")
    driver.wait_for_element('button:contains("AGREE")')
    driver.sleep(0.5)
    print("Closed GDPR notice")
    # Close cookie notice
    try:
        driver.click('button:contains("AGREE")', timeout=3)
    except Exception as e:
        pass

    # Foreach positions
    for pos in tqdm(positions):
        # Move to position
        moveTo(pos)
        driver.sleep(2)

    # Handle all traffic
    print(f"Handle all traffic...")
    for req in raw:
        try:
            response = driver.execute_cdp_cmd(
                "Network.getResponseBody", {"requestId": req["id"]})
            out = {"response": response} | req
            # Save responses
            output.append(out)
        except Exception as e:
            pass
    print(f"Handle traffic done! Output size: {len(output)}")
    raw.clear()

    # Close browser
    driver.close()
    print("Webdriver closed")
    return output

def shipRawParser(raw):
    regex = r"z:(?P<z>\d+)\/X:(?P<x>\d+)\/Y:(?P<y>\d+)"

    shipData = []
    for r in tqdm(raw):
        if "get_data_json_4" in r["path"]:
            matches = re.findall(regex, r["path"])
            z = matches[0][0]
            x = matches[0][1]
            y = matches[0][2]

            rows = json.loads(r["response"]["body"])["data"]["rows"]
            for row in rows:
                data = {}
                data["z"] = z
                data["x"] = x
                data["y"] = y

                data["data"] = row

                shipData.append(data)

    return shipData

def shipDataParser(shipData):
  ships = {}

  for data in tqdm(shipData):
    ship = {} | data["data"]
    ship["TILE_X"] = data["x"]
    ship["TILE_Y"] = data["y"]
    ship["TILE_Z"] = data["z"]

    id = data["data"]["SHIP_ID"]

    # Keep ship with highest Z value
    if id in ships:
      if int(ship["TILE_Z"]) > int(ships[id]["TILE_Z"]):
        ships[id] = ship
    else:
      ships[id] = ship

  return ships


# Load positions from file
with open('positions.json', 'r') as file:
    positions = json.load(file)

print(f'Positions count: {len(positions)}')

# Start parser runner
raw = mtRunner(positions)
print(f"Raw data rows: {len(raw)}")

# Parse raw data
data = shipRawParser(raw)
print(f"Raw ships: {len(data)}")

# Fiter ships
ships = shipDataParser(data)
print(f"Parsed ships: {len(ships)}")

# Save output
with open('ships.json', 'w') as f:
    json.dump(ships, f)