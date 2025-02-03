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
from apify.storages import KeyValueStore
from pathlib import Path
from .rag_pipeline.summary_extractor import SummaryExtractor
from .scraper.scraper import ITAusschreibungScraper, PDFScraper
from dotenv import load_dotenv
import requests
from .rag_pipeline.prompts import Prompts

# To run this Actor locally, you need to have the Selenium Chromedriver installed.
# Follow the installation guide at: https://www.selenium.dev/documentation/webdriver/getting_started/install_drivers/
# When running on the Apify platform, the Chromedriver is already included in the Actor's Docker image.

os.umask(0)
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
        
        save_path= Path('./storage/key_value_stores/documents').absolute().resolve()
        print(save_path)
        prefs = {'download.default_directory' : str(save_path)}
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=chrome_options)

        # Initialize the scraper and summary extractor.
        scraper = ITAusschreibungScraper(driver, actor_input.get('email'), actor_input.get('password'))
        summary_extractor = SummaryExtractor(actor_input.get('hf_api_key'), "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B", "intfloat/multilingual-e5-small")

        # Process the URLs from the request queue.  
        while request := await request_queue.fetch_next_request():
            
            url = request.url
            Actor.log.info(f'Scraping {url} ...')
            try:
                # Navigate to the URL using Selenium WebDriver. Use asyncio.to_thread for non-blocking execution.
                data = await asyncio.to_thread(scraper.scrape, url)
                publication_link = data.get('properties').get('Unterlagen').get('links')[0].get('href')
                publication_content = scraper.download_publication(publication_link )
                documents_link = data.get('properties').get('Einsicht und Anforderung der Verdingungsunterlagen').get('links')[0].get('href')
                pdf_scraper = PDFScraper(driver=driver)
                pdf_scraper.scrape(documents_link)
                with open("publication.pdf", "wb") as f:
                    f.write(publication_content)

                target_path = Path(f'./storage/key_value_stores/documents/{data.get("id")}').absolute().resolve()
                print(target_path)
                os.makedirs(target_path, exist_ok=True)
                time.sleep(1)
                move_files(save_path, target_path)
                summary = summary_extractor.create_summary(publication_content, Prompts.BEKANNTMACHUNG_SUMMARY.value)
                summary_extractor.init_pipeline(list(Path(target_path).glob('*.pdf')))
                detailed_description = summary_extractor.answer_question(Prompts.DOCUMENTS_DESCRIPTION.value)
                data['summary'] = summary
                data['detailed_description'] = detailed_description
                print(f"Detailed Description {detailed_description}")
                # Store the extracted data to the default dataset.
                await Actor.push_data(data)
            except Exception:
                Actor.log.exception(f'Cannot extract data from {url}.')
            finally:
                # Mark the request as handled to ensure it is not processed again.
                await request_queue.mark_request_as_handled(request)
        driver.quit()

def move_files(base_dir, target_dir):
    """
    Moves files with specific extensions from the base directory to the target directory.
    Removes files with other specific extensions from the base directory.
    Args:
        base_dir (str): The path to the base directory containing the files to be moved or removed.
        target_dir (str): The path to the target directory where the files should be moved.
    Supported file extensions for moving:
        - .pdf
        - .json
    Supported file extensions for removing:
        - .docx
        - .doc
        - .zip
        - .xlsx
        - .xls
    Raises:
        Exception: If there is an error moving or removing a file, an error message is printed.
    """

    base_path = Path(base_dir)
    target_path = Path(target_dir)

    for file in base_path.iterdir():  # Iterates over Path objects
        if file.suffix in {".pdf", ".json"}:
            try:
                file.rename(target_path / file.name)
            except Exception as e:
                print(f"Error moving {file.name}: {e}")

        elif file.suffix in {".docx", ".doc", ".zip", ".xlsx", ".xls"}:
            print(f"Removing {file}")
            try:
                file.unlink()  # More intuitive than os.remove
            except Exception as e:
                print(f"Error removing {file.name}: {e}")