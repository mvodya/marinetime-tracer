from seleniumbase import BaseCase
BaseCase.main(__name__, __file__)


class RecorderTest(BaseCase):
    def test_recording(self):
        self.open("https://www.marinetraffic.com/en/ais/home/centerx:132.2/centery:43.0/zoom:10c")
        self.click('button:contains("AGREE")')
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(5)", 179, 227)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(8)", 212, 176)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(3)", 228, 400)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(11)", 203, 473)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(3)", 205, 392)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(4)", 171, 216)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(12)", 22, 363)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(11)", 233, 341)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(13)", 385, 109)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(13)", 446, 450)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(13)", 383, 477)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(6) canvas:nth-of-type(16)", 223, 418)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(4) canvas:nth-of-type(10)", 13, 507)
        self.click("div#map_canvas")
        self.click("div#map_canvas")
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(4) canvas:nth-of-type(6)", 436, 407)
        self.click_with_offset("div#map_canvas div:nth-of-type(11) div:nth-of-type(3) canvas:nth-of-type(3)", 359, 131)
        self.click("body div:nth-of-type(3) ul div:nth-of-type(2) p")
        self.click("body div:nth-of-type(3) ul div:nth-of-type(2) p")
        self.click("svg#svg_icon_filters")
        self.click("svg#svg_icon_filters path")
