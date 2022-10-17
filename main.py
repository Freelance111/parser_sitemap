import pandas as pd
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import certifi
import ssl
from bs4 import BeautifulSoup

def get_sitemap(url):
    """Scrapes an XML sitemap from the provided URL and returns XML source.

    Args:
        url (string): Fully qualified URL pointing to XML sitemap.

    Returns:
        xml (string): XML source of scraped sitemap.
    """
    context = ssl._create_unverified_context()
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
    })

    html = urlopen(req, context=context)

    xml = BeautifulSoup(html,
                         'lxml-xml',
                         from_encoding=html.info().get_param('charset'))

    return xml

def get_sitemap_type(xml):
    """Parse XML source and returns the type of sitemap.

    Args:
        xml (string): Source code of XML sitemap.

    Returns:
        sitemap_type (string): Type of sitemap (sitemap, sitemapindex, or None).
    """

    sitemapindex = xml.find_all('sitemapindex')
    sitemap = xml.find_all('urlset')

    if sitemapindex:
        return 'sitemapindex'
    elif sitemap:
        return 'urlset'
    else:
        return


def get_child_sitemaps(xml):
    """Return a list of child sitemaps present in a XML sitemap file.

    Args:
        xml (string): XML source of sitemap.

    Returns:
        sitemaps (list): Python list of XML sitemap URLs.
    """

    sitemaps = xml.find_all("sitemap")

    output = []

    for sitemap in sitemaps:
        output.append(sitemap.findNext("loc").text)
    return output



def sitemap_to_dataframe(xml, name=None, data=None, verbose=False):
    """Read an XML sitemap into a Pandas dataframe.

    Args:
        xml (string): XML source of sitemap.
        name (optional): Optional name for sitemap parsed.
        verbose (boolean, optional): Set to True to monitor progress.

    Returns:
        dataframe: Pandas dataframe of XML sitemap content.
    """

    df = pd.DataFrame(columns=['loc'])

    urls = xml.find_all("url")

    for url in urls:

        if xml.find("loc"):
            loc = url.findNext("loc")
            if hasattr(loc, "text"):
                loc = loc.text
                parsed_uri = urlparse(loc)
                domain = '{uri.netloc}'.format(uri=parsed_uri)
            else:
                loc = ''
                domain = ''
        else:
            loc = ''
            domain = ''

        row = {
            'domain': domain,
            'loc': loc,
        }

        if verbose:
            print(row)

        df = df.append(row, ignore_index=True)
    return df

# url_finished = "https://themarket.co.uk/finished.xml"
# xml_finished = get_sitemap(url_finished)

def get_all_urls(url):
    """Return a dataframe containing all of the URLs from a site's XML sitemaps.

    Args:
        url (string): URL of site's XML sitemap. Usually located at /sitemap.xml

    Returns:
        df (dataframe): Pandas dataframe containing all sitemap content.

    """

    xml = get_sitemap(url)
    sitemap_type = get_sitemap_type(xml)

    if sitemap_type =='sitemapindex':
        sitemaps = get_child_sitemaps(xml)
    else:
        sitemaps = [url]

    df = pd.DataFrame(columns=['loc'])

    for sitemap in sitemaps:
        sitemap_xml = get_sitemap(sitemap)
        df_sitemap = sitemap_to_dataframe(sitemap_xml, name=sitemap)

        df = pd.concat([df, df_sitemap], ignore_index=True)

    return df


files = [
"https://solaray.no/sitemap.xml",
"https://www.officemanagement.no/sitemap.xml",
"https://kband.no/sitemap.xml",
"https://www.fjellskaal.no/sitemap.xml",
"https://amadeus.com/sitemap.xml",
"https://www.parknordic.no/sitemap.xml",
"https://www.novonordisk.no/sitemap.xml",
"https://kaibosh.com/sitemap.xml",
"https://www.huntinglodge.no/sitemap.xml",
"https://assistertselvhjelp.no/sitemap.xml",
]

urls = []
for file in files:
    # xml = get_sitemap(file)
    xml = len(get_all_urls(file))
    # child_sitemaps = get_child_sitemaps(xml)
    # sitemap_type = get_sitemap_type(xml)
    urls.append(xml)

# df = len(urls)
print(urls)

