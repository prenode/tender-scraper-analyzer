"""This module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

import asyncio
from urllib.parse import urljoin
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
import os
from apify import Actor, Request

from .summary_extractor import SummaryExtractor
from .scraper.scraper import ITAusschreibungScraper, PDFScraper
from dotenv import load_dotenv
import requests

# To run this Actor locally, you need to have the Selenium Chromedriver installed.
# Follow the installation guide at: https://www.selenium.dev/documentation/webdriver/getting_started/install_drivers/
# When running on the Apify platform, the Chromedriver is already included in the Actor's Docker image.


async def main() -> None:
    """Main entry point for the Apify Actor.
    
    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """
    async with Actor:
        # Retrieve the Actor input, and use default values if not provided.
        actor_input = await Actor.get_input() or {}
        print(actor_input)
        start_urls = actor_input.get('start_urls')

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in actor input, exiting...')
            await Actor.exit()

        # Open the default request queue for handling URLs to be processed.
        request_queue = await Actor.open_request_queue()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            url = start_url.get('url')
            Actor.log.info(f'Enqueuing {url} ...')
            request = Request.from_url(url, user_data={'depth': 0})
            await request_queue.add_request(request)

        # Launch a new Selenium Chrome WebDriver and configure it.
        Actor.log.info('Launching Chrome WebDriver...')
        chrome_options = ChromeOptions()

        if Actor.config.headless:
            chrome_options.add_argument('--headless')
        prefs = {'download.default_directory' : '/Users/christopherroth/code/tender-login-actor/storage/files'}
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=chrome_options)
        scraper = ITAusschreibungScraper(driver, actor_input.get('email'), actor_input.get('password'))
        summary_extractor = SummaryExtractor(actor_input.get('hf_api_key'))
    
        # Process the URLs from the request queue.  
        while request := await request_queue.fetch_next_request():
            
            url = request.url
            Actor.log.info(f'Scraping {url} ...')

            try:
                # Navigate to the URL using Selenium WebDriver. Use asyncio.to_thread for non-blocking execution.
                data = await asyncio.to_thread(scraper.scrape, url)

                publication_link = data.get('properties').get('Unterlagen').get('links')[0].get('href')
                print(f"Publication Link: {publication_link}")
                publication_content = scraper.download_publication(publication_link )
                documents_link = data.get('properties').get('Einsicht und Anforderung der Verdingungsunterlagen').get('links')[0].get('href')
                print(f"Document Link: {documents_link}")
                
                pdf_scraper = PDFScraper(driver=driver)
                pdf_scraper.scrape(documents_link)
                #with open("publication.pdf", "rb") as f:
                #    publication_content = f.read()
                with open("publication.pdf", "wb") as f:
                    f.write(publication_content)
                print(f"Downloaded publication {len(publication_content)} bytes")

                summary = summary_extractor.create_summary(publication_content)
                data['summary'] = summary

                # Store the extracted data to the default dataset.
                await Actor.push_data(data)
            except Exception:
                Actor.log.exception(f'Cannot extract data from {url}.')
            finally:
                # Mark the request as handled to ensure it is not processed again.
                await request_queue.mark_request_as_handled(request)
        driver.quit()

