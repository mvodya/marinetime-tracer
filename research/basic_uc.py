from seleniumbase import Driver

import time

driver = Driver(uc=True)
driver.open("https://mvodya.com")

time.sleep(10)

driver.close()