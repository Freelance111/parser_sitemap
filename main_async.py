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

urls_sitemaps = []
amount_sitemaps = []
amount_urls = 0
async def main():
    start_time = time.time()
    with open('urls_sites.txt', 'r') as file_urls_sites:
        try:
            for index, url_site in enumerate(file_urls_sites):
                await asyncio.create_task(get_urls_sitemaps(url_site.strip(), index))

            global urls_sitemaps, amount_sitemaps, amount_urls
            for item in urls_sitemaps:
                amount_urls = 0
                if item[0] in [0, 'server disconnected', 'incorrect response']:
                    amount_urls = 0
                elif len(item) == 1:
                    await asyncio.create_task(get_amount_urls(item[0]))
                else:
                    for url_sitemap in item:
                        await asyncio.create_task(get_amount_urls(url_sitemap))
                        if amount_urls > 10_000:
                            amount_urls = '10_000+'
                            break
                amount_sitemaps.append(amount_urls)

        except Exception as ex:
            print(f'\n\tAn unknown error has occurred:\n{ex}')

    finish_time = time.time() - start_time
    print(f'Script time spent --- {finish_time}')
    create_file()


async def get_urls_sitemaps(url_site, index) -> None:
    urls_sitemaps.append([])
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f'{url_site}robots.txt')
            if resp.status == 404:
                urls_sitemaps[index].append(f'{url_site}sitemap.xml')
                return

            response = await resp.text()
            text = response.strip().split()
            if text.index("Sitemap:"):
                url = text[(text.index("Sitemap:")) + 1]
                if url.find('.xml'):
                    resp = await session.get(url)
                    soup = BeautifulSoup(await resp.read(), 'xml')

                    text = soup.text.strip().split()
                    for item in soup.find_all('loc'):
                        url = item.text
                        urls_sitemaps[index].append(url)
                else:
                    urls_sitemaps[index].append(text[(text.index("Sitemap:")) + 1])
    except ValueError as ex:
        urls_sitemaps[index].append(0)
        print(f'\t{url_site} --- don`t have sitemap')
    except aiohttp.client_exceptions.ClientPayloadError as ex:
        urls_sitemaps[index].append('incorrect response')
        print(f'\n{url_site} --- incorrect response\n')
    except Exception as ex:
        print(f'\n{url_site}\nget_urls_sitemaps:\n{ex}')

async def get_amount_urls(url_sitemap) -> None:
    global amount_urls
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{url_sitemap}') as resp:
                if resp.status == 404:
                    amount_urls = 0
                    print(f'{url_sitemap} --- doesn`t exist')
                    return
                soup = BeautifulSoup(await resp.read(), 'xml')

        text = soup.text.strip().split()
        amount_urls += len(soup.find_all('loc'))
    except aiohttp.client_exceptions.ClientPayloadError as ex:
        amount_urls = 0
        print(f'\n{url_sitemap} --- incorrect response\n')
    except Exception as ex:
        print(f'\n{url_sitemap}\nget_urls_sitemaps:\n{ex}')



def create_file():
    global amount_sitemaps
    with open('data.csv', 'a') as csvfile:
        writer = csv.writer(csvfile)
        with open('urls_sites.txt', 'r', newline='') as file:
            for index, url_site in enumerate(file):
                writer.writerow((url_site.strip(), amount_sitemaps[index]))

if __name__ == '__main__':
    asyncio.run(main())