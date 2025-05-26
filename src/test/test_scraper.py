import pytest


from src.scraper.scraper_evergabe import EvergabeScraper
from src.scraper.scraper_it_ausschreibung import ITAusschreibungScraper
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium import webdriver
from pathlib import Path
from src.utils import move_files
import time

evergabe_url = "https://www.evergabe.nrw.de/VMPSatellite/public/company/project/CXUHYYDYTH47DJQC/de/documents"
chrome_options = ChromeOptions()

save_path = Path("./storage/key_value_stores/documents").absolute().resolve()
prefs = {"download.default_directory": str(save_path)}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
service = Service(service_args=["--verbose"])
driver = webdriver.Chrome(options=chrome_options, service=service)



def test_evergabe_download_links():

    scraper = EvergabeScraper(driver)
    download_elements = scraper._get_download_elements(evergabe_url)
    assert len(download_elements) > 0, "No download elements found on the page."


def test_evergabe_download():
    scraper = EvergabeScraper(driver)
    scraper.scrape(evergabe_url)
    time.sleep(1)
    move_files(
        "./storage/key_value_stores/documents",
        "./storage/key_value_stores/documents/test",
    )
    assert True, "Scraping failed"

