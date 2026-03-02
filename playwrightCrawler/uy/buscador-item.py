import asyncio
from playwright.async_api import async_playwright

async def get_metadata():
    metadata_links = {}
    index = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto("https://visualizador.ide.uy/geonetwork/srv/spa/catalog.search#/search?facet.q=topicCat%2FinlandWaters")
        while True:
            index += 1
            await getItems(page, metadata_links)

            if not await go_to_next_page(page):
                break

        await browser.close()
        write_links_to_file(metadata_links)
    return metadata_links

async def getItems(page, metadata_links):
    links = await page.query_selector_all("a")
    for link in links:
        href = await link.get_attribute("href")
        hrefString = str(href)
        if hrefString.startswith("#/metadata/") and hrefString not in metadata_links.keys():
            metadata_links.update({hrefString: link})
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
    with open("metadata_links.txt", "w") as f:
        for link in links.keys():
            f.write(f"{link}\n")
        
asyncio.run(get_metadata())
