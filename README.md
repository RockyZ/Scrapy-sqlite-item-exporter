Scrapy-sqlite-item-exporter
===========================

Export items to sqlite3 database crawled by scrapy 1.4

How to use
===========================

	1. Place exporters.py to your project script directory.
	2. In settings.py,

		FEED_EXPORTERS = {
		    'sqlite': '<script directory>.exporters.SqliteItemExporter',
		}

	3. In terminal,$ scrapy crawl <spider name> -o sqlite.db -t sqlite