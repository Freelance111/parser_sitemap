import csv
import time
from bs4 import BeautifulSoup
import lxml
import asyncio
import os

os.environ.setdefault('AIOHTTP_NO_EXTENSIONS', "1")
import aiohttp
from asyncio import BoundedSemaphore

with open('data.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Site', 'Amount'])


async def main():
    with open('urls_sites.txt', 'r') as file_urls_sites:
        try:
            tasks = []
            for index, url_site in enumerate(file_urls_sites):
                tasks.append(asyncio.create_task(get_urls_sitemaps(url_site.strip(), index)))


            create_task = time.time()
            urls_sitemaps = await asyncio.gather(*tasks)
            finish_time = time.time() - create_task
            print(f'Script time first part --- {finish_time}')

            print(f'{len(urls_sitemaps)} --- {urls_sitemaps}')

            tasks = []
            for index, item in enumerate(urls_sitemaps):
                try:
                    if item == []:
                        tasks.append(asyncio.create_task(get_amount_urls([0], index)))
                    elif item[0] in ["don't have sitemap", 'incorrect response', 'response size too large',
                                   'ConnectionResetError', 'ServerDisconnectedError']:
                        tasks.append(asyncio.create_task(get_amount_urls(item[0], index)))
                    elif len(item) == 1:
                        tasks.append(asyncio.create_task(get_amount_urls(item, index)))
                    else:
                        tasks.append(asyncio.create_task(get_amount_urls(item, index)))
                except IndexError:
                    print(f'{item} --- {index}')

            create_task = time.time()
            amount_sitemaps = await asyncio.gather(*tasks)
            finish_time = time.time() - create_task
            print(f'Script time second part --- {finish_time}')


        except ValueError as ex:
            print(f'\n\tAn unknown error has occurred:\n{ex}')

    create_file(amount_sitemaps)

async def get_urls_sitemaps(url_site, index):
    urls_sitemaps = []

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f'{url_site}robots.txt')
            all_urls_sitemap = []
            if resp.status in [404, 403]:
                all_urls_sitemap.append(f'{url_site}sitemap.xml')
            # elif resp.status == 403:
            #     print(f'{url_site} --- Site access denied')
            #     all_urls_sitemap.append(f'Site access denied')
            #     return
            else:
                response = await resp.text()

                text = response.strip().split()
                search = 'Sitemap:'
                if response.find('sitemap:') > 0:
                    search = 'sitemap:'
                while True:
                    index_sitemap = text.index(search)
                    text.remove(search)
                    all_urls_sitemap.append([text[index_sitemap]])
                    if text.count(search) == 0:
                        break

        urls_sitemaps = await check_sitemap(all_urls_sitemap, index)
        if urls_sitemaps == []:
            urls_sitemaps.append(0)
        else:
            return urls_sitemaps

    except ValueError as ex:
        print(f'\t{url_site} --- don`t have sitemap')
        urls_sitemaps.append("don't have sitemap")
    except aiohttp.client_exceptions.ClientPayloadError as ex:
        print(f'{url_site} --- incorrect response')
        urls_sitemaps.append('incorrect response')
    except aiohttp.client_exceptions.ClientResponseError as ex:
        print(f'{url_site} --- response size too large')
        urls_sitemaps.append('response size too large')
    except ConnectionResetError:
        print(f'{url_site} --- ConnectionResetError')
        urls_sitemaps.append('ConnectionResetError')
    except aiohttp.client_exceptions.ServerDisconnectedError:
        print(f'{url_site} --- ServerDisconnectedError')
        urls_sitemaps.append('ServerDisconnectedError')
    # except Exception as ex:
    #     print(f'\n{url_site} get_urls_sitemaps:\n\t{ex}')
    #     urls_sitemaps.append('Error')
    finally:
        return urls_sitemaps



async def check_sitemap(urls, index):
    urls_sitemap = []

    try:
        for url in urls:
            if len(urls) == 1:
                urls_loc = await get_amount_urls(urls, None, False)
            else:
                urls_loc = await get_amount_urls(url, None, False)


            check = urls_loc[0].text.find('xml')
            if check > 0:
                for item in urls_loc:
                    urls_sitemap.append(item.text)
            elif check == -1:
                urls_sitemap.append(urls[0])
                return
    finally:
        return urls_sitemap




async def get_amount_urls(urls, index, default=True):
    amount = 0
    urls_loc = []
    for url in urls:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'{url}') as resp:
                    if resp.status == 404:
                        if default:
                            print(f'{url} --- doesn`t exist')
                        else:
                            print(f'{url} --- error 404')
                            urls_loc.append('error 404')
                        continue

                    soup = BeautifulSoup(await resp.text(), 'xml')

            urls_loc = soup.find_all('loc')
            if default:
                amount += len(urls_loc)
                if amount > 10_000:
                    amount = '10_000+'
                    return

        except aiohttp.client_exceptions.ClientPayloadError as ex:
            print(f'{url} --- incorrect response')
        except ConnectionResetError:
            print(f'{url} --- ConnectionResetError')
        # except Exception as ex:
        #     print(f'\n{url} get_amount_urls:\n\t{ex}')
        finally:
            if default:
                return amount
            else:
                return urls_loc


def create_file(amount_sitemaps):
    with open('data.csv', 'a') as csvfile:
        writer = csv.writer(csvfile)
        print(str(len(amount_sitemaps)) + ' --- ' + str(amount_sitemaps))
        with open('urls_sites.txt', 'r', newline='') as file:
            for index, url_site in enumerate(file):
                writer.writerow((url_site.strip(), amount_sitemaps[index]))




if __name__ == '__main__':
    asyncio.run(main())