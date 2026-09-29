from playwright.sync_api import sync_playwright
import os

html_path = os.path.abspath(r"docs/assets/architecture.html")
file_uri = "file:///" + html_path.replace("\\", "/")
png_path = os.path.abspath("docs/assets/architecture.png")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 840})
    page.goto(file_uri)
    # Give time for Google Fonts to load
    page.wait_for_timeout(1000)
    
    # Target the diagram container
    container = page.locator(".diagram-container")
    container.screenshot(path=png_path)
    browser.close()

print(f"Screenshot saved to {png_path}")
