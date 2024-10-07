from seleniumbase import BaseCase
BaseCase.main(__name__, __file__)


class RecorderTest(BaseCase):
    def test_recording(self):
        self.open("https://www.marinetraffic.com/en/ais/home/centerx:132.2/centery:43.0/zoom:10c")
        self.click('button:contains("AGREE")')
        self.click("body div:nth-of-type(3) ul div:nth-of-type(2) p")
        self.click("body div:nth-of-type(3) ul div:nth-of-type(2) p")
