"""This module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from selenium.webdriver.chrome.service import Service

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
from .scraper.scraper_router import ScraperRouter
# from .rag_pipeline.summary_extractor import RAGPipeline
# from .rag_pipeline.prompts import Prompts

from .scraper.scraper_it_ausschreibung import ITAusschreibungScraper
from dotenv import load_dotenv
import requests
from .document_storage.document_storage import S3DocumentStorage

storage = S3DocumentStorage()


async def main() -> None:
    """Main entry point for the Apify Actor.

    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """
    async with Actor:
        # Retrieve the Actor input, and use default values if not provided.
        actor_input = await Actor.get_input() or {}
        start_urls = actor_input.get("start_urls")

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info("No start URLs specified in actor input, exiting...")
            await Actor.exit()

        # Open the default request queue for handling URLs to be processed.
        request_queue = await Actor.open_request_queue()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            url = start_url.get("url")
            Actor.log.info(f"Enqueuing {url} ...")
            request = Request.from_url(url, user_data={"depth": 0})
            await request_queue.add_request(request)

        # Launch a new Selenium Chrome WebDriver and configure it.
        Actor.log.info("Launching Chrome WebDriver...")
        chrome_options = ChromeOptions()

        save_path = Path("./storage/key_value_stores/documents").absolute().resolve()
        prefs = {"download.default_directory": str(save_path)}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        service = Service(service_args=["--verbose"])

        driver = webdriver.Chrome(options=chrome_options, service=service)

        # Initialize the scraper and summary extractor.
        # scraper = ITAusschreibungScraper(
        #     driver, actor_input.get("email"), actor_input.get("password")
        # )
        # summary_extractor = RAGPipeline(actor_input.get('hf_api_key'), "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B", "intfloat/multilingual-e5-small")

        # Process the URLs from the request queue.
        while request := await request_queue.fetch_next_request():
            url = request.url
            scraper = ScraperRouter(
                driver, actor_input.get("email"), actor_input.get("password")
            ).get_scraper(url)
            Actor.log.info(f"Scraping {url} ...")
            # Navigate to the URL using Selenium WebDriver. Use asyncio.to_thread for non-blocking execution.
            data = await asyncio.to_thread(scraper.scrape, url)
            try:
                scraper.download_publication()
                storage.upload_file(
                    f"./storage/key_value_stores/documents/{data.get('id')}/publication/publication.pdf",
                    f"{data.get('id')}/publication/publication.pdf",
                )
            except ValueError as e:
                Actor.log.exception(e)
                continue
            except Exception as e:
                Actor.log.exception(e)
                continue

            target_path = (
                Path(f"./storage/key_value_stores/documents/{data.get('id')}")
                .absolute()
                .resolve()
            )
            move_files(save_path, target_path)
            storage.upload_files(list(target_path.glob("*.pdf")))
            await Actor.push_data(data)
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
    os.makedirs(target_path, exist_ok=True)
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
