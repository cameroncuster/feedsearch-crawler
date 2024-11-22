from feedsearch_crawler import search

if __name__ == "__main__":
    feeds = search("blog.statsig.com")
    print(feeds)
