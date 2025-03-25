from src.scraper.scraper_it_ausschreibung import ITAusschreibungScraper
import re


sources = {
    "https://www.it-ausschreibung.de": ITAusschreibungScraper,
    "https://www.evergabe.nrw.de": None,
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
        # regex to identify classic top level domain, should be in format: https://www.example.com
        if re.match(r"https://www\.[a-zA-Z0-9-]+\.[a-z]{2,3}", url):
            url = re.match(r"https://www\.[a-zA-Z0-9-]+\.[a-z]{2,3}", url).group(0)
            # check if sources has key with url
            if sources.get(url) is None:
                # return BaseScraper if no scraper is available
                return None
            else:
                return sources[url](self.driver, self.email, self.password)
        else:
            raise ValueError("Not a valid url!")
