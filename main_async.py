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
    amount = int(input('How many sites in a document: '))
    data = [0] * amount
    amount_sitemaps = []
    try:
        urls_sitemaps = []
        for item in range(0, amount - 100, 100):
            print(item)
            create_task = time.time()
            urls_sitemaps.extend(await asyncio.gather(*(generat_sites(item, item+100))))
            finish_time = time.time() - create_task
            print(f'Script time --- {finish_time}')



        print(f'{len(urls_sitemaps)} --- {urls_sitemaps}')

        with open('urls_sites.txt', 'r') as file:
            for index, url_site in enumerate(file):
                tasks = []
                try:
                    print(index)
                    if urls_sitemaps[index] == []:
                        dict = {
                            'url_site': 0
                        }
                        data.insert(index, dict)
                    elif urls_sitemaps[index] in ["don't have sitemap", 'incorrect response', 'response size too large',
                                   'ConnectionResetError', 'ServerDisconnectedError']:
                        dict = {
                            'url_site': urls_sitemaps[index]
                        }
                        data.insert(index, dict)
                    else:
                        tasks.append(asyncio.create_task(get_amount_urls(urls_sitemaps[index], index)))

                except IndexError:
                    print(f'{urls_sitemaps[index]} --- {index}')

                if index == amount - 1:
                    break

        create_task = time.time()
        amount_sitemaps = await asyncio.gather(*tasks)
        finish_time = time.time() - create_task
        print(f'Script time second part --- {finish_time}')

    except ValueError as ex:
        print(f'\n\tAn unknown error has occurred:\n{ex}')



    for dict in amount_sitemaps:
        for key, value in dict.items():
            data.insert(int(key), value)
    create_file(data)


def generat_sites(start, finish):
    with open('urls_sites.txt', 'r') as file_urls_sites:
        tasks = []
        for index, url_site in enumerate(file_urls_sites):
            if index >= start:
                tasks.append(asyncio.create_task(get_urls_sitemaps(url_site.strip(), index)))
            if index == finish - 1:
                tasks.append(asyncio.create_task(get_urls_sitemaps(url_site.strip(), index)))
                return tasks


async def get_urls_sitemaps(url_site, index):
    urls_sitemaps = []

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f'{url_site}robots.txt')
            all_urls_sitemap = []
            if resp.status in [404, 403]:
                all_urls_sitemap.append(f'{url_site}sitemap.xml')
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
    except Exception as ex:
        print(f'\n{url_site} get_urls_sitemaps:\n\t{ex}')
        urls_sitemaps.append('Error')
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
                dict = {f'{index}': amount}
                return dict
            else:
                return urls_loc


def create_file(data):
    with open('data.csv', 'a') as csvfile:
        writer = csv.writer(csvfile)
        print(str(len(data)) + ' --- ' + str(data))
        with open('urls_sites.txt', 'r', newline='') as file:
            for index, url_site in enumerate(file):
                writer.writerow((url_site.strip(), data[index]))




if __name__ == '__main__':
    asyncio.run(main())