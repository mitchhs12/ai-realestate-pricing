import argparse
import asyncio
import random
from itertools import cycle

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup


# Load proxies from proxies.txt
def load_proxies():
    with open('proxies.txt', 'r') as file:
        return [line.strip() for line in file.readlines()]

async def fetch_page(session, url, proxy=None):
    """Fetch the HTML content of a page with retries using different proxies (if provided)."""
    retry_count = 5  # Number of retries
    for _ in range(retry_count):
        try:
            async with session.get(url, proxy=proxy, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    print(f"Failed with status {response.status}. Retrying...")
        except Exception as e:
            print(f"Error: {e}. Retrying...")
    return None  # Return None if all retries fail

def parse_page(html):
    """Parse the HTML content and extract property listings."""
    soup = BeautifulSoup(html, 'lxml')
    listings = soup.find_all("a", class_=[
        "ember-view block h-full py-6 px-3 md:px-6 text-slateGrey-500",
        "ember-view listing-tile-info flex flex-col justify-between h-full p-3 xl:py-4 xl:px-6.25 text-slateGrey-500"
    ])
    properties = []
    for listing in listings:
        link = listing.get("href")

        # Extract listing date
        date = listing.find("div", {"data-test": "tile__search-result__content__date"})
        listing_date = " ".join(date.stripped_strings).replace("Listed", "").strip() if date else "N/A"
        
        # Extract address
        address = listing.find("h3", {"data-test": "premium-tile__search-result__address"})
        if not address:
            address = listing.find("h3", {"data-test": "standard-tile__search-result__address"})
        address_text = address.text.strip() if address else "N/A"
        
        # Extract price
        price = listing.find("div", {"data-test": "price-display__price-method"})
        price_text = price.text.strip() if price else "N/A"
        
        # Extract features
        bedrooms = listing.find("div", {"data-test": "bedroom"})
        num_bedrooms = bedrooms.text.strip() if bedrooms else "N/A"

        bathrooms = listing.find("div", {"data-test": "bathroom"})
        num_bathrooms = bathrooms.text.strip() if bathrooms else "N/A"

        land_area = listing.find("div", {"data-test": "land-area"})
        if land_area:
            land_text = land_area.text.strip()
            if land_text.endswith("m2"):
                # Remove "m2" and return the number
                land_size = land_text.replace("m2", "").strip()
            elif land_text.endswith("ha"):
                # Convert hectares (ha) to square meters (m2)
                land_size = land_text.replace("ha", "").strip()
                try:
                    land_size = str(int(float(land_size) * 10000))  # Convert ha to m2 and remove decimals
                except ValueError:
                    land_size = "N/A"
            else:
                land_size = "N/A"
        else:  
            land_size = "N/A"

        properties.append({
            "link": "https://www.realestate.co.nz" + link,
            "listed": listing_date,
            "address": address_text,
            "price": price_text,
            "bedrooms": num_bedrooms,
            "bathrooms": num_bathrooms,
            "area (m2)": land_size,
        })

    return properties

async def scrape_page(session, url, page_number, proxy=None):
    """Scrape a single page."""
    print(f"Scraping page {page_number}...")
    html = await fetch_page(session, url, proxy)
    if html:
        properties = parse_page(html)
        print(f"Found {len(properties)} listings on page {page_number}.")
        return properties
    else:
        print(f"No data found on page {page_number}.")
        return []

async def scrape_all_pages_concurrently(base_url, max_pages, proxies=None):
    """Scrape all pages concurrently."""
    if proxies is None:
        raise ValueError("Proxies list must not be None.")

    async with aiohttp.ClientSession() as session:
        tasks = []
        
        for page_number in range(1, max_pages + 1):
            url = f"{base_url}&page={page_number}" if page_number > 1 else base_url
            proxy = random.choice(proxies)
            tasks.append(scrape_page(session, url, page_number, proxy))
        
        # Run tasks concurrently and gather results in order
        results = await asyncio.gather(*tasks)
        
        # Flatten the list of results (each result is a list of properties)
        all_properties = [property for sublist in results for property in sublist]
        return all_properties

async def scrape_all_pages_regular(base_url, max_pages):
    """Scrape all pages with regular requests (no proxies)."""
    async with aiohttp.ClientSession() as session:
        all_properties = []
        zero_count = 0

        for page_number in range(1, max_pages + 1):
            url = f"{base_url}&page={page_number}" if page_number > 1 else base_url
            properties = await scrape_page(session, url, page_number)
            if not properties:
                zero_count += 1
            else:
                zero_count = 0
            all_properties.extend(properties)
            print(f"Total properties found so far: {len(all_properties)}")

            if zero_count >= 2:
                print("No properties found on 2 consecutive pages. Stopping...")
                break
        return all_properties

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape real estate listings.")
    parser.add_argument('--mode', type=str, choices=['concurrent', 'regular'], default='concurrent',
                        help="Specify 'concurrent' for concurrent requests or 'regular' for regular requests.")
    args = parser.parse_args()

    base_url = "https://www.realestate.co.nz/residential/sale/wellington?by=latest"
    max_pages = 500

    # Run the scraper based on the mode
    print("Starting scraping...")
    if args.mode == 'concurrent':
        proxies = load_proxies()
        all_properties = asyncio.run(scrape_all_pages_concurrently(base_url, max_pages, proxies))
    else:
        all_properties = asyncio.run(scrape_all_pages_regular(base_url, max_pages))

    # Create a Pandas DataFrame from the scraped data
    df = pd.DataFrame(all_properties)

    # Export the DataFrame to a CSV file
    df.to_csv("real_estate_listings.csv", index=False)

    print("Data has been scraped and saved to 'real_estate_listings.csv'")
    print(df)