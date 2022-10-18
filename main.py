import csv
import time
from bs4 import BeautifulSoup
import lxml
import asyncio
import os

os.environ.setdefault('AIOHTTP_NO_EXTENSIONS', "1")
import aiohttp

with open('data.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Site', 'Amount'])

sites_data = []
urls_sitemaps = []

async def main():
    start_time = time.time()
    with open('urls_sites.txt', 'r') as file_urls_sites:
        try:
            tasks_get_urls_sitemaps = []
            for url_site in file_urls_sites:
                task = asyncio.create_task(get_urls_sitemaps(url_site.strip()))
                tasks_get_urls_sitemaps.append(task)

            await asyncio.gather(*tasks_get_urls_sitemaps)

            tasks_get_amount_urls = []
            for url_sitemap in urls_sitemaps:
                task = asyncio.create_task(get_amount_urls(url_sitemap))
                tasks_get_amount_urls.append(task)

            await asyncio.gather(*tasks_get_amount_urls)
        except Exception as ex:
            print(f'\n\tAn unknown error has occurred:\n{ex}')

    finish_time = time.time() - start_time
    print(f'Script time spent --- {finish_time}')

    await create_file()


async def get_amount_urls(url_sitemap):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{url_sitemap}') as resp:
            if resp.status == 404:
                print(f'{url_sitemap} --- doesn`t exist')
                return

            soup = BeautifulSoup(await resp.text(), 'xml')

    text = soup.text.strip().split()
    amount_urls = len(soup.find_all('loc'))
    if amount_urls > 10_000:
        amount_urls = '10_000 +'

    sites_data.append({
        f'{url_sitemap.strip()}': amount_urls,
    })


async def get_urls_sitemaps(url_site):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{url_site}robots.txt') as resp:
                response = await resp.text()
                if resp.status == 404:
                    urls_sitemaps.append(f'{url_site}sitemap.xml')
                    return

        text = response.strip().split()
        if text[(text.index("Sitemap:")) + 1]:
            urls_sitemaps.append(text[(text.index("Sitemap:")) + 1])
    except ValueError as ex:
        print(f'\t{url_site} --- don`t have sitemap')


async def create_file():
    with open('data.csv', 'a') as csvfile:
        writer = csv.writer(csvfile)

        for site_data in sites_data:
            for key, value in site_data.items():
                writer.writerow((key, value))


if __name__ == '__main__':
    asyncio.run(main())
