from seleniumbase import Driver

import json
import re
from tqdm import tqdm
from datetime import datetime
from selenium.webdriver.common.by import By

# Download data from marine traffic
def mtRunner(positions):
    output = []

    print("Open webdriver")
    driver = Driver(uc=True, uc_cdp_events=True, headless=False)
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
        req["timestamp"] = datetime.now().timestamp()
        if "/getData/" in req["path"]:
            raw.append(req)

    # Listen to network events
    driver.add_cdp_listener('Network.requestWillBeSentExtraInfo', save)

    # Open marine traffic
    print("Open marine traffic")
    driver.open(
        "https://www.marinetraffic.com/en/ais/home/centerx:132.2/centery:43.0/zoom:10")
    driver.sleep(5)

    for attempt in range(5):
        # Fix window handle
        driver.switch_to.window(driver.window_handles[0])
        try:
            # Wait for GDPR notice and agree
            print("Looking for cookie agreement button")
            driver.wait_for_element_visible(
                By.XPATH, "//button[span='AGREE']", timeout=15)
            driver.click(By.XPATH, "//button[span='AGREE']")
            print("Cookie notice closed")
            print("Looking for map canvas")
            # Wait for map area
            driver.wait_for_element_visible(
                By.XPATH, "//div[@id='map_canvas']", timeout=15)
            print("Looking for any tile")
            driver.wait_for_element_visible(
                By.XPATH, "//canvas[contains(@class, 'leaflet-tile-loaded')]", timeout=15)
            break
        except Exception as e:
            # Show error
            print(f"Error while loading window (attempt #{attempt})\n{e}")
            if (attempt == 4):
                raise Exception("Error while loading window")
            driver.switch_to.window(driver.window_handles[0])
            driver.refresh()

    print("Run movement process...")

    # Iterate over positions
    for pos in tqdm(positions):
        # Move to position
        moveTo(pos)
        driver.sleep(2)

    # Handle all traffic
    print(f"Handling all traffic...")
    for req in raw:
        try:
            response = driver.execute_cdp_cmd(
                "Network.getResponseBody", {"requestId": req["id"]})
            out = {"response": response} | req
            # Save responses
            output.append(out)
        except Exception as e:
            pass
    print(f"Handling traffic done! Output size: {len(output)}")
    raw.clear()

    # Close browser
    try:
        driver.close()
    except Exception as e:
        pass
    print("Webdriver closed")
    return output

# Parse network data
def shipRawParser(raw):
    # Position pattern
    regex = r"z:(?P<z>\d+)\/X:(?P<x>\d+)\/Y:(?P<y>\d+)"

    # Get all ships
    shipData = []
    for r in tqdm(raw):
        if "get_data_json_4" in r["path"]:
            matches = re.findall(regex, r["path"])
            z = matches[0][0]
            x = matches[0][1]
            y = matches[0][2]

            # Read body elements
            rows = json.loads(r["response"]["body"])["data"]["rows"]
            for row in rows:
                data = {}
                data["z"] = z
                data["x"] = x
                data["y"] = y
                data["timestamp"] = r["timestamp"]
                data["data"] = row
                # Add ship to array
                shipData.append(data)
    return shipData

# Handle all ships
def shipDataParser(shipData):
    # Keep unique ships (remove all dublications)
    ships = {}

    for data in tqdm(shipData):
        ship = {} | data["data"]
        ship["TILE_Z"] = data["z"]
        ship["TIMESTAMP"] = data["timestamp"] - \
            (float(ship["ELAPSED"]) if ship["ELAPSED"] else 0)

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

# Filter ships
ships = shipDataParser(data)
print(f"Parsed ships: {len(ships)}")

# Save output
with open('data.json', 'w') as f:
    json.dump(ships, f)
