from src.scraper.scraper_it_ausschreibung import ITAusschreibungScraper
from src.scraper.scraper_evergabe import EvergabeScraper
from src.scraper.scraper_dtvp import DTVPScraper
import re
from urllib.parse import urlparse


sources = {
    "https://www.evergabe.nrw.de": EvergabeScraper,
    "https://www.dtvp.de": DTVPScraper,
    "https://www.deutsche-evergabe.de": None,
    "https://www.meinauftrag.rib.de": None,
    "https://ausschreibungen.landbw.de": None,
}


class ScraperRouter:
    def __init__(self, driver, email, password):
        self.driver = driver
        self.email = email
        self.password = password

    def get_scraper(self, url):
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        scraper_class = sources.get(base_url)
        if scraper_class is None:
            return None
        return scraper_class(self.driver)
