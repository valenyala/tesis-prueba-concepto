import asyncio
import os
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, '..', 'ch_metadata_links.txt')

async def get_metadata():
    metadata_links = {}
    index = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(
            "https://www.geocat.ch/geonetwork/srv/eng/catalog.search#/search?facet.q=topicCat%2FinlandWaters",
            wait_until='networkidle', timeout=30000
        )
        await asyncio.sleep(2)
        while True:
            index += 1
            await getItems(page, metadata_links)

            if not await go_to_next_page(page) or index >= 20:
                break


        await browser.close()
        write_links_to_file(metadata_links)
    return metadata_links

async def getItems(page, metadata_links):
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href")
        hrefString = str(href)
        if '#/metadata/' in hrefString:
            fragment = '#' + hrefString.split('#')[1]  # extract #/metadata/{uuid}
            if fragment not in metadata_links:
                metadata_links[fragment] = link
    return metadata_links

async def go_to_next_page(current_page):
    next_button = current_page.locator('[data-ng-click="next()"]').first
    parent = next_button.locator('xpath=..')
    if await parent.get_attribute("class") != "disabled":
        await next_button.click()
        await current_page.wait_for_load_state('networkidle')
        return True
    return False

def write_links_to_file(links):
    with open(OUTPUT_FILE, "w") as f:
        for link in links.keys():
            f.write(f"{link}\n")
    print(f"Wrote {len(links)} links to {OUTPUT_FILE}")

asyncio.run(get_metadata())
