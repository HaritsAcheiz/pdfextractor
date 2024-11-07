from httpx import AsyncClient, Client
from dataclasses import dataclass
from selectolax.parser import HTMLParser
import logging
import asyncio
import os
import duckdb
import json
from urllib.parse import urljoin
import re

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class SeasonalJobsScraper:
	base_url: str = 'https://seasonaljobs.dol.gov'
	api_endpoint = 'https://api.seasonaljobs.dol.gov/datahub/search?api-version=2020-06-30'
	user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'

	async def fetch(self, aclient, url, limit, mode=None, payload=None):
		logger.info(f'Fetching {url}...')
		if mode == 'detail':
			async with limit:
				response = await aclient.get(url)
				if limit.locked():
					await asyncio.sleep(1)
					response.raise_for_status()
			logger.info(f'Fetching {url}...Completed!')

			return url, response.text
		elif mode == 'search':
			async with limit:
				response = await aclient.post(url, json=payload)
				if limit.locked():
					await asyncio.sleep(1)
					response.raise_for_status()
			logger.info(f'Fetching {url}...Completed!')

			return url, response.text

	async def fetch_all(self, urls=None, mode=None, payloads=None):
		tasks = []
		headers = {
			'user-agent': self.user_agent
		}
		limit = asyncio.Semaphore(4)
		async with AsyncClient(headers=headers, timeout=120) as aclient:
			if mode == 'detail':
				for url in urls:
					task = asyncio.create_task(
						self.fetch(
							aclient,
							url=url,
							limit=limit,
							mode=mode
						)
					)
					tasks.append(task)
				htmls = await asyncio.gather(*tasks)
			elif mode == 'search':
				for payload in payloads:
					task = asyncio.create_task(
						self.fetch(
							aclient,
							url=self.api_endpoint,
							limit=limit,
							mode=mode,
							payload=payload
						)
					)
					tasks.append(task)
				htmls = await asyncio.gather(*tasks)
		return htmls

	def insert_to_db(self, htmls, database_name, table_name):
		logger.info('Inserting data to database...')
		if os.path.exists(database_name):
			os.remove(database_name)

		conn = duckdb.connect(database_name)
		curr = conn.cursor()

		try:
			curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT, html BLOB)")

			htmls = list()
			for url, html in htmls:
				if not isinstance(html, bytes):
					htmls.append(url, bytes(html, 'utf-8'))
				else:
					htmls.append(url, html)

			curr.executemany(f"INSERT INTO {table_name} (url, html) VALUES (?, ?)", htmls)
			conn.commit()

		finally:
			curr.close()
			conn.close()
			logger.info('Data inserted!')

	def get_count_of_data(self):
		headers = {
			'user-agent': self.user_agent
		}

		with Client(headers=headers) as client:
			response = client.get(target_url)
			response.raise_for_status()

		print(response.text)
		tree = HTMLParser(response.text)
		page_text = tree.css_first('p.text-xs.font-bold').text(strip=True)
		match = re.search(r'of (\d+) Results', page_text)
		if match:
			result = int(match.group(1))
			return result
		else:
			logger.info("No match found")

	def get_job_links(self):
		count_of_data = self.get_count_of_data()

		with open('payload.json', 'r') as file:
			payload = json.load(file)

		print(payload)

		# search_result = asyncio.run(self.fetch_all(urls))

		# return search_result

	def get_jobs_data(self):
		pass

	def main(self):
		search_result = self.get_job_links()
		# print(search_result)