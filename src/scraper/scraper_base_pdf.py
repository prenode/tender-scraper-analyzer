from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import requests
import time
from selenium.webdriver.support.wait import WebDriverWait
from threading import Thread
from selenium.common.exceptions import TimeoutException



class PDFScraper(BaseScraper):
    '''
    PDFScraper is a class that extends BaseScraper to handle the downloading of PDF files from a webpage using a Selenium WebDriver.
    Methods:
        __init__(driver: webdriver.Chrome):
            Initializes the PDFScraper with a Selenium WebDriver.
        _download_element(url):
            Downloads a file from the given URL and prints the file object.
        _get_download_elements():
            Retrieves all downloadable file links from the current page and prints their text.
        scrape(url: str):
            Scrapes the given URL for downloadable files and attempts to download them.
    '''
    
    def __init__(self, driver: webdriver.Chrome):
        super().__init__(driver)

    def _download_element(self, url):
        """
        Downloads a file from the given URL and saves it to the specified file path.
        Args:
            url (str): The URL of the file to download.
            file_path (str): The path where the file should be saved.
        """
        file = url.click()

    def _get_download_elements(self):
        """
        Retrieves all downloadable file links from the current page.
        Returns:
            list: A list of URLs of downloadable files.
        """
        #element contains title 'herunterladen' or 'Download'
        elements = self.driver.find_elements("xpath", "//*[ (@title and contains(translate(@title, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download')) or  (@title and contains(translate(@title, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunterladen')) or ( (self::a or self::button or @role='button' or contains(@class, 'button') or contains(@class, 'btn')) and  (contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'download') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'herunterladen') ))]")
        if len(elements) == 0:
            raise Exception("No download elements found on the page.")
        return elements

    def scrape(self, url: str):
        """
        Scrapes the given URL for downloadable files and saves them.
        Args:
            url (str): The URL to scrape for downloadable files.
        """
        self.driver.get(url)
        download_links = self._get_download_elements()
        if len(download_links) == 0:
            return False
        else: 
            for index, link in enumerate(download_links):
                # file_extension = link.split('.')[-1]
                # file_path = f"downloaded_file_{index + 1}.{file_extension}"
                self._download_element(link)
        return True