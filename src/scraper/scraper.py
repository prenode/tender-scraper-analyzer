from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import requests

def _element_with_text_exists(driver, xpath):
    try:
        driver.find_element(By.XPATH, xpath)
        return True
    except Exception:
        return False

class BaseScraper: 

    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver

    def scrape(self):
        return NotImplementedError("This method must be implemented in a subclass!")
    
    def login_driver(self):
        return NotImplementedError("This method must be implemented in a subclass!")
    

    
class ITAusschreibungScraper(BaseScraper):
    '''
    ITAusschreibungScraper is a class that extends BaseScraper to scrape tender information from the website 'https://www.it-ausschreibung.de/'.
    Attributes:
    Methods:
        __init__(driver: webdriver.Chrome):
            Initializes the scraper with a Chrome WebDriver instance and logs in using the provided credentials.
        scrape(url: str) -> dict:
            Scrapes tender information from the given URL and returns it as a dictionary.
        _is_logged_in() -> bool:
            Checks if the user is logged in to the website by looking for an element containing the text 'Mein Konto'.
        _login(email: str, password: str):
            Logs into the website using the provided email and password.
        logged_in_driver(email: str, password: str) -> webdriver.Chrome:
            Logs in to the web application using the provided email and password, attempts to load cookies to maintain the session, and saves cookies for future use.
        _load_cookies():
            Loads cookies from a JSON file and adds them to the web driver.
        _save_cookies(driver: webdriver.Chrome):
    '''
    
    def __init__(self, driver: webdriver.Chrome, email: str, password: str):
        super().__init__(driver)
        self.logged_in_driver(email=email, password=password)

    def scrape(self, url: str):
        self.driver.get(url)
        header = self.driver.find_element(By.XPATH, "//h1")
        tender_name = header.text.removeprefix('Ausschreibung "').removesuffix('"')

        header_2 = self.driver.find_element(By.XPATH, "//h2")
        tender_id = header_2.text.removeprefix("Details zur Ausschreibung ")

        tender = {
            "name": tender_name,
            "id": tender_id,
            "properties": {}
        }

        card_body = self.driver.find_element(By.XPATH, "//div[@class='card-body']")
        headers = card_body.find_elements(By.XPATH, "//h3")
        for header in headers:
            property_name = header.text.strip().removesuffix(":")
            sibling = header.find_element(By.XPATH, "following-sibling::*")
            property_content = sibling.text.strip()
            links = sibling.find_elements(By.TAG_NAME, "a")
            tender["properties"][property_name] = {
                "content": property_content,
                "links": [{"text": link.text, "href": link.get_attribute("href")} for link in links]
            }

        return tender

    def _is_logged_in(self):
        """
        Check if the user is logged in to the website.
        This function navigates to the specified website and checks if an element
        containing the text 'Mein Konto' exists, which indicates that the user is
        logged in.
        Args:
            driver (webdriver.Chrome): The Selenium WebDriver instance for Chrome.
        Returns:
            bool: True if the user is logged in, False otherwise.
        """

        self.driver.get("https://www.it-ausschreibung.de/")
        self.driver.implicitly_wait(2)
        return _element_with_text_exists(self.driver, "//*[contains(text(), 'Mein Konto')]")
    

    def _login(self, email, password):
        """
        Logs into the website 'https://www.it-ausschreibung.de/' using the provided email and password.
        Args:
            email (str): The email address to use for login.
            password (str): The password to use for login.
        Returns:
            None
        """
        self.driver.get('https://www.it-ausschreibung.de/')
        login_button = self.driver.find_element(by=By.LINK_TEXT, value='Einloggen')
        login_button.click()
        self.driver.implicitly_wait(2)

        email_input = self.driver.find_element(by=By.NAME, value="email")
        for s in email:
            email_input.send_keys(s)
            self.driver.implicitly_wait(1)

        password_input = self.driver.find_element(by=By.NAME, value="password")
        for s in password:
            password_input.send_keys(s)
            self.driver.implicitly_wait(1)

        stay_logged_in = self.driver.find_element(by=By.NAME, value="remember")
        stay_logged_in.click()

        login_button= self.driver.find_element(By.XPATH, "//button[contains(text(), 'Einloggen')]")
        login_button.click()

        self.driver.implicitly_wait(5)

    def logged_in_driver(self, email, password):
        """
        Logs in to the web application using the provided email and password.
        This method attempts to load cookies to maintain the session. If the cookies are not valid or the user is not logged in,
        it will perform the login process using the provided credentials and save the cookies for future use.
        Args:
            email (str): The email address used for logging in.
            password (str): The password used for logging in.
        Returns:
            webdriver.Chrome: The Chrome WebDriver instance after logging in.
        Raises:
            Exception: If the login process fails.
        """   
        try:
            self._load_cookies()
        except Exception as e:
            print("Failed to load cookies: ", e)
        
        if not self._is_logged_in():
            print("Not logged in yet. Logging in...")
            self._login(email, password)
            self._save_cookies()
        
        if not self._is_logged_in():
            raise Exception("Failed to login")
    
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

        self.driver.get("https://www.it-ausschreibung.de/")
        with open('cookies.json', 'r') as f:
            cookies = json.load(f)

        for cookie in cookies:
            self.driver.add_cookie(cookie)
        
        self.driver.refresh()
        self.driver.implicitly_wait(5)

    def _save_cookies(self):
        """
        Saves the cookies from the given Chrome WebDriver instance to a JSON file.
        Args:
            driver (webdriver.Chrome): The Chrome WebDriver instance from which to retrieve cookies.
        Saves:
            A JSON file named 'cookies.json' containing the cookies.
        """
        
        cookies = self.driver.get_cookies()
        with open('cookies.json', 'w') as f:
            json.dump(cookies, f)

    def download_publication(self, link: str) -> bytes:
        """
        Downloads a publication from the given link using the provided Selenium WebDriver.
        Args:
            link (str): The URL of the publication to download.
            driver (webdriver.Chrome): An instance of Selenium WebDriver for Chrome.
        Returns:
            bytes: The content of the downloaded publication in bytes.
        Raises:
            Exception: If the downloaded file is not a PDF.
            requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        """

        cookies = self.driver.get_cookies()
        cookies = {cookie["name"]:cookie["value"] for cookie in cookies if "www.it-ausschreibung.de" in cookie["domain"]}
        response = requests.get(link, cookies=cookies)
        response.raise_for_status()
        if response.headers.get("Content-Type") != "application/pdf":
            raise Exception(f"File is not a PDF ({response.headers.get('Content-Type')})")
        return response.content


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
        print(file)
        # with open(file_path, 'wb') as file:
        #     file.write(self.driver.page_source.encode('utf-8'))

    def _get_download_elements(self):
        """
        Retrieves all downloadable file links from the current page.
        Returns:
            list: A list of URLs of downloadable files.
        """
        elements = self.driver.find_elements("xpath", '//*[contains(@title, "herunterladen")]')

        # Print or interact with elements
        for element in elements:
            print(element.text)  # or element.get_attribute("href") if they are links

        return elements

    def scrape(self, url: str):
        """
        Scrapes the given URL for downloadable files and saves them.
        Args:
            url (str): The URL to scrape for downloadable files.
        """
        self.driver.get(url)
        download_links = self._get_download_elements()

        for index, link in enumerate(download_links):
            # file_extension = link.split('.')[-1]
            # file_path = f"downloaded_file_{index + 1}.{file_extension}"
            self._download_element(link)