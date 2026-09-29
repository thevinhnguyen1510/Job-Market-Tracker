import re
import os

html_path = r"C:\Users\ntvin\.gemini\antigravity\brain\81a0219b-aaa8-4c5c-bd12-d112b8a1726d\architecture.html"
with open(html_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Extract SVG
svg_match = re.search(r'(<svg[^>]*>)(.*?)(</svg>)', content, re.DOTALL)
if not svg_match:
    print("SVG not found")
    exit(1)

svg_open = svg_match.group(1)
svg_inner = svg_match.group(2)
svg_close = svg_match.group(3)

# Extract styles
style_match = re.search(r'<style>(.*?)</style>', content, re.DOTALL)
styles = style_match.group(1) if style_match else ""

# Add xmlns if missing
if 'xmlns="http://www.w3.org/2000/svg"' not in svg_open:
    svg_open = svg_open.replace('<svg', '<svg xmlns="http://www.w3.org/2000/svg"')

# Inject styles (WITHOUT external font imports, which violate GitHub/VSCode SVG CSP)
injected_style = f"<defs><style>{styles}</style></defs>"

final_svg = f"{svg_open}\n{injected_style}\n{svg_inner}\n{svg_close}"

with open("docs/assets/architecture.svg", "w", encoding="utf-8") as f:
    f.write(final_svg)

print("SVG extracted to docs/assets/architecture.svg")
