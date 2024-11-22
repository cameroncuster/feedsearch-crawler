import asyncio
import logging
import json
import time
from pprint import pprint
from feedsearch_crawler import search, FeedsearchSpider, output_opml, sort_urls
from feedsearch_crawler.crawler import coerce_url
from datetime import datetime
import collections


urls = [
    "http://www.wetpaint.com/",
    "https://www.zoho.com",
    "http://www.digg.com",
    "https://www.meta.com",
    "http://www.omnidrive.com",
    "http://www.geni.com",
    "http://www.flektor.com",
    "https://www.fox.com/",
    "https://twitter.com/home?lang=en",
    "http://www.stumbleupon.com/",
    "http://gizmoz.com",
    "http://www.scribd.com",
    "http://www.slacker.com",
    "http://www.lala.com",
    "http://www.helio.com",
    "http://ebay.com",
    "http://wis.dm",
    "http://www.meetmoi.com",
    "http://postini.com",
    "http://joost.com",
    "http://www.hutchison-whampoa.com/",
    "http://www.ckh.com.hk/",
    "https://www.lksf.org",
    "https://www.paramount.com/brand/cbs-entertainment",
    "https://www.paramount.com",
    "http://babelgum.com",
    "http://www.plaxo.com/",
    "http://www.cisco.com",
    "http://powerset.com",
    "http://technorati.com",
    "http://www.addthis.com",
    "https://www.openx.com/",
    "http://mahalo.com",
    "http://sparter.com",
    "http://kyte.de.tl/",
    "https://www.warnermedia.com/",
    "https://www.goldmansachs.com",
    "http://thoof.com",
    "http://jinglenetworks.com",
    "http://www.hearst.com",
    "https://strands.com",
    "https://www.ning.com",
    "http://www.lifelock.com",
    "http://wesabe.com",
    "http://www.prosper.com",
    "https://www.youtube.com/",
    "http://www.blogtv.com",
    "https://livestream.com/",
    "http://www.justin.tv",
    "https://video.ibm.com",
    "http://www.tapuz.co.il",
    "http://grandcentral.com",
    "http://www.ikan.net",
    "https://www.fortunebusinessinsights.com/",
    "http://topix.com",
    "http://www.tribunemedia.com/",
    "http://www.gannett.com",
    "http://www.jobster.co.uk",
    "http://www.pownce.com/",
    "http://www.revision3.com",
    "http://www.allpeers.com/",
    "http://www.aggregateknowledge.com",
    "http://zing.net",
    "http://www.criticalmetrics.com",
    "http://zenzui.com",
    "http://www.spock.com",
    "http://wize.com",
    "http://sodahead.com",
    "http://hotornot.com",
    "http://www.popsugar.com",
    "https://www.nbcuniversal.com/",
    "http://jajah.com",
    "http://www.skype.com",
    "http://gizmo5.com",
    "http://www.fring.com",
    "http://iskoot.com",
    "http://www.eqo.com",
    "http://allofmp3.com",
    "http://amiestreet.com",
    "http://www.sellaband.com",
    "http://funnyordie.com",
    "http://steorn.com",
    "http://surphace.com",
    "http://icontact.com",
    "http://meevee.com",
    "http://www.blinkx.com",
    "http://zlio.com",
    "http://yelp.com",
    "http://www.jaiku.com",
    "http://tun3r.com",
    "http://www.leggmason.com",
    "http://www.tripup.com",
    "http://coghead.com",
    "http://zooomr.com",
    "https://www.kayak.co.in",
    "http://www.farecast.com/",
    "http://www.yapta.com",
    "http://www.dailymotion.com",
    "http://www.kickapps.com",
    "http://www.rockyou.com",
]


def get_pretty_print(json_object: object):
    return json.dumps(json_object, sort_keys=True, indent=2, separators=(",", ": "))


# @profile()
def run_crawl():
    # user_agent = "Mozilla/5.0 (Compatible; Bot)"
    user_agent = "Mozilla/5.0 (Compatible; Feedsearch Bot)"
    # user_agent = "curl/7.58.0"
    # user_agent = (
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0"
    # )
    # user_agent = (
    #     "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    # )

    # headers = {
    #     "User-Agent": user_agent,
    #     "DNT": "1",
    #     "Upgrade-Insecure-Requests": "1",
    #     "Accept-Language": "en-US,en;q=0.5",
    #     "Accept-Encoding": "gzip, deflate, br",
    #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    #     "Referrer": "https://www.google.com/",
    # }

    crawler = FeedsearchSpider(
        concurrency=10,
        total_timeout=100000,
        request_timeout=100000,
        user_agent=user_agent,
        # headers=headers,
        favicon_data_uri=False,
        max_depth=1,
        max_retries=3,
        ssl=True,
        full_crawl=False,
        delay=0,
        try_urls=True,
    )
    crawler.start_urls = urls
    # crawler.allowed_domains = create_allowed_domains(urls)
    asyncio.run(crawler.crawl())
    # asyncio.run(crawler.crawl(urls[0]))
    # items = search(urls, crawl_hosts=True)

    items = sort_urls(list(crawler.items))

    serialized = [item.serialize() for item in items]

    # items = search(urls[0], concurrency=40, try_urls=False, favicon_data_uri=False)
    # serialized = [item.serialize() for item in items]

    results = get_pretty_print(serialized)
    print(results)

    site_metas = [item.serialize() for item in crawler.site_metas]
    metas = get_pretty_print(site_metas)
    print(metas)
    # pprint(site_metas)

    pprint(crawler.favicons)
    pprint(crawler._duplicate_filter.fingerprints)

    print(output_opml(items).decode())

    pprint([result["url"] for result in serialized])
    pprint(crawler.get_stats())

    print(f"Feeds found: {len(items)}")
    print(f"SiteMetas: {len(crawler.site_metas)}")
    print(f"Favicons fetched: {len(crawler.favicons)}")
    # pprint(crawler.queue_wait_times)
    pprint(list((x.score, x.url) for x in items))


def create_allowed_domains(urls):
    domain_patterns = []
    for url in urls:
        url = coerce_url(url)
        host = url.host
        pattern = f"*.{host}"
        domain_patterns.append(host)
        domain_patterns.append(pattern)
    return domain_patterns


if __name__ == "__main__":
    logger = logging.getLogger("feedsearch_crawler")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s [in %(pathname)s:%(lineno)d]"
    )
    ch.setFormatter(formatter)
    fl = logging.FileHandler(
        f"/Users/cameroncuster/exposit-ai/feedsearch-crawler/logs/feedsearch_crawl_{datetime.utcnow().isoformat()}"
    )
    fl.setLevel((logging.DEBUG))
    fl.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fl)

    start = time.perf_counter()

    run_crawl()

    duration = int((time.perf_counter() - start) * 1000)
    print(f"Entire process ran in {duration}ms")
