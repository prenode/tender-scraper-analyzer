# Title = Die Datei herunterladen
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import requests
import time
from selenium.webdriver.support.wait import WebDriverWait
from threading import Thread
from selenium.common.exceptions import TimeoutException
import os
from .scraper_it_ausschreibung import BaseScraper


class EvergabeScraper(BaseScraper):
    """
    ITAusschreibungScraper is a class that extends BaseScraper to scrape tender information from the website 'https://www.it-ausschreibung.de/'.
    Attributes:
        s3_bucket_name (str): Name of the S3 bucket to store downloaded documents
        s3_document_storage (S3DocumentStorage): Storage client for saving documents to S3
    Methods:
        __init__(driver: webdriver.Chrome, email: str, password: str, s3_bucket_name: str):
            Initializes the scraper with a Chrome WebDriver instance, logs in using credentials,
            and sets up S3 storage for documents.
        scrape(url: str) -> dict:
            Scrapes tender information from the given URL and returns it as a dictionary.
            Downloads and stores any associated documents in S3.
        _is_logged_in() -> bool:
            Checks if the user is logged in to the website by looking for an element containing the text 'Mein Konto'.
        _login(email: str, password: str):
            Logs into the website using the provided email and password.
        logged_in_driver(email: str, password: str) -> webdriver.Chrome:
            Logs in to the web application using the provided email and password, attempts to load cookies to maintain the session, and saves cookies for future use.
        _load_cookies():
            Loads cookies from a JSON file and adds them to the web driver.
        _save_cookies():
            Saves the current session cookies to a JSON file.
        download_publication(link: str) -> bytes:
            Downloads a publication from the given link and stores it in S3.
    """

    def __init__(self, driver: webdriver.Chrome):
        super().__init__(driver)

    def scrape(self, url: str):
        """
        The downloadable
        Args:
            url (str): _description_

        Returns:
            _type_: _description_
        """
        
        download_elements = self._get_download_elements(url)
        if len(download_elements) == 0:
            raise Exception("No download elements found on the page.")
        for element in download_elements:
            file = self._download_element(element)
        pass
    
    def _download_element(self, url):
        """
        Downloads a file from the given URL and saves it to the specified file path.
        Args:
            url (str): The URL of the file to download.
            file_path (str): The path where the file should be saved.
        """
        file = url.click()
        return file

    def _get_download_elements(self, url):
        """
        Retrieves all downloadable file links from the current page.
        Returns:
            list: A list of URLs of downloadable files.
        """
        
        self.driver.get(url)
        # element contains title 'Die Datei herunterladen'
        elements = self.driver.find_elements(
            "xpath",
            "//*[@title='Die Datei herunterladen']"
        )
        print(f"Found {len(elements)} download elements.")
        return elements


    def _is_logged_in(self):
        # No login required
        pass

    def _login(self, email, password):
        # No login required
        pass

    def logged_in_driver(self, email, password):
        return self.driver # No login required
        

    def _load_cookies(self):
        """
        Load cookies from a JSON file and add them to the web driver.
        This method navigates to the specified URL, reads cookies from a
        'cookies.json' file, adds each cookie to the web driver, refreshes
        the page, and sets an implicit wait time.
        Raises:
            FileNotFoundError: If the 'cookies.json' file does not exist.
            json.JSONDecodeError: If the 'cookies.json' file is not a valid JSON.
        """

        pass

    def _save_cookies(self):
        pass
        