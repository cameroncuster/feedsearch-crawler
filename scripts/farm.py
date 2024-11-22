"""
Script to farm feeds from the organizations list

TODO integrate the bing search api to fetch company blogs
"""

import logging
import os
import csv
import json
import time
import sys
from tqdm import tqdm
from feedsearch_crawler import search
from datetime import datetime


# I'm still not getting debug logs
logger = logging.getLogger("feedsearch_crawler")
logger.setLevel(logging.DEBUG)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class BatchFeedFinder:
    def __init__(self, timeout: int = 60, batch_size: int = 100):
        self.timeout = timeout
        self.batch_size = batch_size
        logger.info(
            f"Initialized BatchFeedFinder with timeout={timeout}, batch_size={batch_size}"
        )

    def find_feeds_batch(self, urls: list) -> list:
        logger.debug(f"Starting batch search for {len(urls)} URLs")
        print(urls)
        try:
            logger.debug(f"Sample URLs from batch: {urls[:5]}")
            feeds = search(
                urls,
                threads=256,
                total_timeout=self.timeout * len(urls),
                request_timeout=100000,  # infinite in practice
                favicon_data_uri=False,
                crawl_hosts=True,
                try_urls=True,
            )

            if feeds:
                logger.debug(f"Found {len(feeds)} raw feeds")
                processed_feeds = [
                    {
                        "url": str(feed.url),
                        "velocity": getattr(feed, "velocity", None),
                        "item_count": getattr(feed, "item_count", None),
                        "last_updated": getattr(feed, "last_updated", None),
                        "score": getattr(feed, "score", 0),
                        "version": getattr(feed, "version", None),
                    }
                    for feed in feeds
                    if getattr(feed, "bozo", 0) == 0
                ]
                logger.debug(f"Processed {len(processed_feeds)} feeds")
                return processed_feeds
            else:
                logger.debug("No feeds found in batch")
                return []
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}", exc_info=True)
            return []


def process_csv(input_file: str, output_file: str, cursor_file: str):
    finder = BatchFeedFinder()

    # Ensure cursor file exists
    if not os.path.exists(cursor_file):
        logger.info(f"Creating new cursor file at {cursor_file}")
        with open(cursor_file, "w") as f:
            f.write("0")

    # Load cursor
    with open(cursor_file, "r") as f:
        start_index = int(f.read().strip() or 0)
        logger.info(f"Resuming from index {start_index}")

    # Read URLs
    urls_to_process = []
    company_data = {}
    logger.info(f"Reading URLs from {input_file}")
    with open(input_file, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            url = row.get("homepage_url", "").strip().lower()
            if url:
                urls_to_process.append(url)
                company_data[url] = {
                    "company_name": row.get("name", ""),
                    "domain": row.get("domain", ""),
                }

    total_urls = len(urls_to_process)
    total_remaining = total_urls - start_index
    logger.info(
        f"Found {total_urls} total URLs, {total_remaining} remaining to process"
    )

    # Always create progress bar to stderr
    pbar = tqdm(
        total=total_remaining,
        desc=f"Processing URLs ({start_index}/{total_urls})",
        unit="urls",
        file=sys.stderr,
    )

    try:
        with open(output_file, "a", encoding="utf-8") as outfile:
            for i in range(start_index, len(urls_to_process), finder.batch_size):
                batch = urls_to_process[i : i + finder.batch_size]
                batch_results = []

                logger.debug(f"Processing batch starting at index {i}")
                logger.debug(f"Batch size: {len(batch)}")

                try:
                    feeds = finder.find_feeds_batch(batch)
                    logger.info(
                        f"Batch {i}: Got {len(feeds)} feeds for {len(batch)} URLs"
                    )

                    # Match feeds back to their URLs and collect results
                    for feed in feeds:
                        feed_url = str(feed.get("url", "")).lower()
                        matched = False
                        logger.debug(f"Attempting to match feed URL: {feed_url}")

                        for url in batch:
                            # More flexible matching
                            try:
                                url_domain = url.split("//")[-1].split("/")[0]
                                feed_domain = feed_url.split("//")[-1].split("/")[0]
                                logger.debug(
                                    f"Comparing domains: {url_domain} vs {feed_domain}"
                                )

                                if (
                                    url_domain in feed_domain
                                    or feed_domain in url_domain
                                ):
                                    result = {
                                        "url": url,
                                        "feeds": [feed],
                                        **company_data[url],
                                    }
                                    batch_results.append(result)
                                    matched = True
                                    logger.debug(f"Matched feed to URL: {url}")
                                    break
                            except Exception as e:
                                logger.error(f"Error matching URL {url}: {str(e)}")
                                continue

                        if not matched:
                            logger.debug(
                                f"Could not match feed URL {feed_url} to any source URLs in batch"
                            )

                    logger.info(f"Batch {i}: Matched {len(batch_results)} results")

                    # Write all results and update cursor atomically
                    results_written = 0
                    for result in batch_results:
                        try:
                            outfile.write(
                                json.dumps(result, cls=DateTimeEncoder) + "\n"
                            )
                            results_written += 1
                        except Exception as e:
                            logger.error(f"Error writing result: {str(e)}")
                            continue

                    outfile.flush()
                    logger.debug(f"Wrote {results_written} results to file")

                    # Update cursor
                    try:
                        with open(cursor_file, "w") as f:
                            new_position = i + len(batch)
                            f.write(str(new_position))
                            logger.debug(f"Updated cursor to {new_position}")
                    except Exception as e:
                        logger.error(f"Error updating cursor: {str(e)}")

                except Exception as e:
                    logger.error(
                        f"Error processing batch starting at index {i}: {str(e)}",
                        exc_info=True,
                    )
                    continue
                finally:
                    # Always update progress bar
                    pbar.update(len(batch))
                    pbar.set_description(
                        f"Processing URLs ({i + len(batch)}/{total_urls})"
                    )

    except Exception as e:
        logger.error(f"Fatal error occurred: {str(e)}", exc_info=True)
    finally:
        pbar.close()
        logger.info("Processing completed")


if __name__ == "__main__":
    INPUT_CSV = (
        "/Users/cameroncuster/exposit-ai/feedsearch-crawler/data/organizations.csv"
    )
    OUTPUT_FILE = "/Users/cameroncuster/exposit-ai/feedsearch-crawler/data/organizations_feeds.jsonl"
    CURSOR_FILE = "/Users/cameroncuster/exposit-ai/feedsearch-crawler/data/organizations_cursor.txt"

    while True:
        try:
            logger.info("Starting feed search process")
            process_csv(INPUT_CSV, OUTPUT_FILE, CURSOR_FILE)
            logger.info("Process completed successfully")
            break

        except KeyboardInterrupt:
            logger.warning("Process interrupted by user")
            sys.exit(1)

        except Exception as e:
            logger.error(
                f"Process crashed, will restart from cursor: {str(e)}", exc_info=True
            )
            time.sleep(5)
            logger.info("Restarting process...")
            continue
