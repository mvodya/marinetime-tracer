from seleniumbase import Driver

from rich.pretty import pprint

driver = Driver(uc=True, uc_cdp_events=True)

driver.add_cdp_listener(
    "Network.requestWillBeSentExtraInfo",
    lambda data: pprint(data)
)

driver.execute_cdp_cmd('Network.enable', {})

driver.uc_open_with_reconnect(
    "https://www.marinetraffic.com/en/ais/home/centerx:132.2/centery:43.0/zoom:10", 2)

driver.sleep(2)

driver.click('button:contains("AGREE")')

driver.sleep(2)

driver.close()