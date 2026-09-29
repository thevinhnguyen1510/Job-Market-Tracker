from playwright.sync_api import sync_playwright
import os

svg_path = os.path.abspath(r"docs/assets/architecture.svg")
file_uri = "file:///" + svg_path.replace("\\", "/")
png_path = os.path.abspath("docs/assets/architecture.png")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1240, "height": 660})
    page.goto(file_uri)
    # Give a tiny bit of time for fonts to render (system-ui is fast though)
    page.wait_for_timeout(500)
    page.screenshot(path=png_path)
    browser.close()

print(f"Screenshot saved to {png_path}")
