"""
Helper functions related to scraping images.
"""
from aqt.qt import QApplication
from typing import NamedTuple, List, Tuple
import io
import re
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from anki.utils import checksum
from io import BytesIO
from typing import Tuple
from math import ceil

from .logging import logger

class QueryResult(NamedTuple):
    """
    Encapsulates all of the information and configs needed to process a query result and apply
    the changes back into the Anki database.
    """
    note_id: str
    query: str
    target_field: str
    overwrite: str
    max_results: int
    width: int
    height: int
    # (filename, image data)
    images: List[Tuple[str, bytes]]


def sleep(seconds):
    """
    Sleep for a certain amount of time to throttle request rates.
    """
    start = time.time()
    while time.time() - start < seconds:
        time.sleep(0.01)
        QApplication.instance().processEvents()


def strip_html_clozes(w: str) -> str:
    """
    Strips a string of any HTML and clozes.

    In particular, this is used as note fields can have a lot of random stuff on
    them that we don't want to enter into the search query.
    """
    # This code is copy-pasted straight from
    # batch-download-pictures-from-google-images
    # Unfortunately it's more or less unreadable/unmaintainable and I'm just going
    # to trust that it works.
    w = re.sub(r'</?(b|i|u|strong|span)(?: [^>]+)>', '', w)
    w = re.sub(r'\[sound:.*?\]', '', w)
    if '<' in w:
        soup = BeautifulSoup(w, "html.parser")
        for s in soup.stripped_strings:
            w = s
            break
        else:
            w = re.sub(r'<br ?/?>[\s\S]+$', ' ', w)
            w = re.sub(r'<[^>]+>', '', w)

    clozes = re.findall(r'{{c\d+::(.*?)(?::.*?)?}}', w)
    if clozes:
        w = ' '.join(clozes)
    return w


class Scraper:
    # Taken from the source code of bing-image-downloader (Python)
    # Note: It's actually really important that these headers have certain
    # values, though I can't say what they are.
    # https://github.com/gurugaurav/bing_image_downloader/commit/062e0f4e6ec31b0ab66ea10e0a187d1d2d80de63
    # https://github.com/gurugaurav/bing_image_downloader/issues/19
    # But as it turns out, using the headers from the first commit there solves
    # the problem.
    SPOOFED_HEADER = {
        'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'}

    def __init__(self, executor: concurrent.futures.ThreadPoolExecutor, mw):
        self._executor = executor
        self._mw = mw

    def push_scrape_job(self, result: QueryResult):
        """
        Pushes a new job (future) into the executor using the query result.

        Returns a Future.
        """
        raise Exception("Unimplemented abstract method.")


class BingScraper(Scraper):
    """
    A scraper that targets Bing Images.

    This can be refactored if we ever choose to add another source. Things such as
    retry logic can be extracted into a common class.
    """
    SEARCH_FORMAT_URL = "https://www.bing.com/images/async?q={}"
    TIMEOUT_SEC = 15
    MAX_RETRIES = 3
    # Number of seconds to sleep per retry on rate limit error.
    THROTTLE_SLEEP_SEC = 30
    # Number of seconds to sleep per retry on timeout error.
    TIMEOUT_SLEEP_SEC = 5

    # Taken from bing-image-downloader
    IMAGE_URL_REGEX = 'murl&quot;:&quot;(.*?)&quot;'

    def __init__(self, executor: concurrent.futures.ThreadPoolExecutor, mw):
        super().__init__(executor, mw)

    def push_scrape_job(self, result: QueryResult):
        """
        Fire off a request to the image search page, then queue up a job to scrape
        the images from the resulting text and resize them.
        """
        # Note that the REQUEST is not
        # multithreaded, but parsing/extracting images is (disputable whether this
        # is the correct architecture, but I'm just going to copy this guy's code).
        # In case of a status exception, retry
        search_url = BingScraper.SEARCH_FORMAT_URL.format(result.query)
        retry_count = 0
        while retry_count < BingScraper.MAX_RETRIES:
            try:
                req = requests.get(search_url, headers=Scraper.SPOOFED_HEADER,
                                   timeout=BingScraper.TIMEOUT_SEC)
                req.raise_for_status()
                future = self._executor.submit(
                    self._parse_and_download_images, req.text, result)
                return future
            except requests.exceptions.RequestException as e:
                logger.exception(e)
                if retry_count == BingScraper.MAX_RETRIES:
                    raise Exception(
                        "Exceeded max retries. Unable to scrape for query: %s" %
                        result.query)
                retry_count += 1

                if isinstance(
                        e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                    # Retry on 429: we were rate limited
                    self._mw.progress.update(
                        f"Sleeping for {retry_cnt * 30} seconds...")
                    QApplication.instance().processEvents()
                    sleep(retry_cnt * BingScraper.THROTTLE_SLEEP_SEC)
                elif isinstance(e, (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError)):
                    # Connection error
                    self._mw.progress.update(
                        f"Sleeping for {retry_cnt * 5} seconds...")
                    QApplication.instance().processEvents()
                    sleep(retry_cnt * BingScraper.TIMEOUT_SLEEP_SEC)
                else:
                    raise e

    def _parse_and_download_images(
            self,
            html: str,
            result: QueryResult) -> QueryResult:
        """
        Parses the image URLs out of the HTML. Processes and resizes them.

        This function **mutates** `result` and also returns it.
        """
        image_urls = re.findall(BingScraper.IMAGE_URL_REGEX, html)
        num_processed = 0
        if len(image_urls) == 0:
            logger.debug("Found 0 image URLs for query: %s" % result.query)

        for url in image_urls:
            if num_processed == result.max_results:
                break

            try:
                req = requests.get(url,
                                   headers=Scraper.SPOOFED_HEADER,
                                   timeout=BingScraper.TIMEOUT_SEC)
                req.raise_for_status()
            except requests.packages.urllib3.exceptions.LocationParseError:
                continue
            except requests.exceptions.RequestException:
                continue

            # Ignore SVGs. Dunno, the last guy did it too, maybe they won't work
            # with Anki.
            if 'image/svg+xml' in req.headers.get('content-type', ''):
                continue

            try:
                buf = _maybe_resize_image(io.BytesIO(req.content), result.width,
                                          result.height)
            except UnicodeError as e:
                # UnicodeError: encoding with 'idna' codec failed (UnicodeError: label empty or too long)
                # https://bugs.python.org/issue32958
                continue

            filename = checksum(url + result.query)
            result.images.append((filename, buf.getvalue()))
            num_processed += 1

        return result


def _maybe_resize_image(image_data: BytesIO, user_width: int, user_height: int) -> BytesIO:
    should_resize = user_width > 0 or user_height > 0

    if should_resize:
        img_data = BytesIO(image_data.read())
        img_data.seek(0)
        img = Image.open(img_data)
        width, height = img.size
        if user_width > 0 and user_height > 0:
            img = resize_proportional(img, user_width, user_height)
        elif user_width > 0:
            img = resize_proportional(img, user_width, height)
        elif user_height > 0:
            img = resize_proportional(img, width, user_height)
        img_data_resized = BytesIO()
        img.save(img_data_resized, format="JPEG")
        img_data_resized.seek(0)
        # Now, you can use img_data_resized for whatever you want
        print("Image resized")
        return img_data_resized
    else:
        # No need to resize
        print("No need to resize")
        return image_data

def resize_proportional(img, new_width, new_height):
    width, height = img.size
    ratio = min(new_width / width, new_height / height)
    return img.resize((ceil(width * ratio), ceil(height * ratio)), Image.ANTIALIAS)

def image_resize(image_data: BytesIO, width: int, height: int) -> BytesIO:
    img_data = BytesIO(image_data.read())
    img_data.seek(0)
    img = Image.open(img_data)
    img.thumbnail((width, height))
    img_resized = BytesIO()
    img.save(img_resized, format="JPEG")
    img_resized.seek(0)
    return img_resized
