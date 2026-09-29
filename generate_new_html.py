import os

html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Job Market Tracker · Architecture</title>
  <link href="https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --color-paper: #f5f5f5; --color-paper-2: #ececec;
      --color-ink: #2d3142; --color-muted: #4f5d75; --color-soft: #7a8399;
      --color-rule: rgba(45,49,66,0.12);
      --color-accent: #eb6c36;
      --color-link: #2e5aa8;
      --font-sans: 'Geist', system-ui, sans-serif;
      --font-serif: 'Instrument Serif', serif;
      --font-mono: 'Geist Mono', ui-monospace, monospace;
    }
    body { font-family: var(--font-sans); background: var(--color-paper); min-height: 100vh; padding: 3rem 2rem; color: var(--color-ink); }
    .container { max-width: 1400px; margin: 0 auto; }
    .header { margin-bottom: 2.5rem; }
    .header-eyebrow { font-family: var(--font-mono); font-size: 0.66rem; font-weight: 500; letter-spacing: 0.18em; text-transform: uppercase; color: var(--color-muted); margin-bottom: 0.75rem; }
    h1 { font-family: var(--font-serif); font-size: clamp(1.75rem, 3vw + 1rem, 2.5rem); font-weight: 400; letter-spacing: -0.02em; line-height: 1.1; margin-bottom: 0.5rem; }
    .subtitle { font-size: 1rem; line-height: 1.55; color: var(--color-muted); max-width: 65ch; }
    .diagram-container { background: var(--color-paper-2); border-radius: 8px; border: 1px solid var(--color-rule); padding: 1.5rem; overflow-x: auto; display: flex; justify-content: center; }
    svg { width: 100%; min-width: 1340px; display: block; max-width: 1340px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <p class="header-eyebrow">Architecture · Diagram Design</p>
      <h1>Lakehouse &amp; RAG Pipeline</h1>
      <p class="subtitle">Complete Medallion Architecture (Bronze/Silver/Gold) paired with Qdrant Hybrid Search, BAAI Reranker, and GPT-4o for Enterprise AI Recruitment.</p>
    </div>

    <div class="diagram-container">
      <svg viewBox="0 0 1340 640" xmlns="http://www.w3.org/2000/svg" role="img">
        <defs>
          <pattern id="dots" width="22" height="22" patternUnits="userSpaceOnUse">
            <circle cx="1" cy="1" r="0.9" fill="rgba(45,49,66,0.10)"/>
          </pattern>
          <marker id="arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#4f5d75"/></marker>
          <marker id="arrow-accent" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#eb6c36"/></marker>
          <marker id="arrow-link" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#2e5aa8"/></marker>
          <marker id="arrowstart-ai" markerWidth="8" markerHeight="6" refX="1" refY="3" orient="auto"><polygon points="8 0, 0 3, 8 6" fill="#7c3aed"/></marker>
          <marker id="arrowhead-ai" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#7c3aed"/></marker>
        </defs>

        <rect width="100%" height="100%" fill="#f5f5f5"/>
        <rect width="100%" height="100%" fill="url(#dots)" opacity="0.55"/>

        <!-- ZONES -->
        <rect x="24" y="64" width="200" height="232" rx="8" fill="rgba(45,49,66,0.02)" stroke="rgba(45,49,66,0.10)" stroke-width="0.8"/>
        <rect x="74" y="68" width="100" height="12" rx="2" fill="#f5f5f5"/>
        <text x="124" y="77" fill="rgba(45,49,66,0.40)" font-size="7" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.14em">ACQUISITION</text>

        <rect x="288" y="64" width="464" height="360" rx="8" fill="rgba(45,49,66,0.02)" stroke="rgba(45,49,66,0.10)" stroke-width="0.8"/>
        <rect x="433" y="68" width="174" height="12" rx="2" fill="#f5f5f5"/>
        <text x="520" y="77" fill="rgba(45,49,66,0.40)" font-size="7" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.14em">DUCKDB LAKEHOUSE (dbt)</text>

        <rect x="816" y="64" width="464" height="512" rx="8" fill="rgba(45,49,66,0.02)" stroke="rgba(45,49,66,0.10)" stroke-width="0.8"/>
        <rect x="961" y="68" width="174" height="12" rx="2" fill="#f5f5f5"/>
        <text x="1048" y="77" fill="rgba(45,49,66,0.40)" font-size="7" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.14em">VECTOR ENGINE &amp; RAG UI</text>

        <!-- ARROWS -->
        <path d="M 124,152 V 232" fill="none" stroke="#4f5d75" stroke-width="1.2" marker-end="url(#arrow)"/>
        <path d="M 208,256 H 248 Q 256,256 256,248 V 136 Q 256,128 264,128 H 304" fill="none" stroke="#4f5d75" stroke-width="1.2" marker-end="url(#arrow)"/>
        <path d="M 428,152 V 232" fill="none" stroke="#4f5d75" stroke-width="1.2" marker-end="url(#arrow)"/>
        <path d="M 348,232 V 152" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrowhead-ai)"/>
        <path d="M 472,128 H 568" fill="none" stroke="#2e5aa8" stroke-width="1.4" marker-end="url(#arrow-link)"/>
        <path d="M 620,152 V 320 Q 620,328 612,328 H 396 Q 388,328 388,336 V 360" fill="none" stroke="#2e5aa8" stroke-width="1.4" marker-end="url(#arrow-link)"/>
        <path d="M 736,128 H 832" fill="none" stroke="#eb6c36" stroke-width="1.4" marker-end="url(#arrow-accent)"/>
        <path d="M 1000,128 H 1096" fill="none" stroke="#eb6c36" stroke-width="1.4" marker-end="url(#arrow-accent)"/>
        <path d="M 684,152 V 360" fill="none" stroke="#eb6c36" stroke-width="1.4" marker-end="url(#arrow-accent)"/>
        <path d="M 568,384 H 528 Q 520,384 520,376 V 336 a 8,8 0 0,1 0,-16 V 200 a 8,8 0 0,0 -8,-8 H 396 Q 388,192 388,184 V 152" fill="none" stroke="#eb6c36" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrow-accent)"/>
        <path d="M 472,396 H 496 Q 504,396 504,404 V 452 Q 504,460 512,460 H 1064 Q 1072,460 1072,452 V 392 Q 1072,384 1080,384 H 1096" fill="none" stroke="#2e5aa8" stroke-width="1.4" marker-end="url(#arrow-link)"/>
        
        <!-- RAG Arrows -->
        <path d="M 208,536 H 304" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrowhead-ai)"/>
        <path d="M 472,536 H 568" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrowhead-ai)"/>
        <path d="M 652,512 V 488 Q 652,480 660,480 H 1040 Q 1048,480 1048,472 V 468 a 8,8 0 0,1 0,-16 V 208 a 8,8 0 0,1 8,-8 H 1180 Q 1188,200 1188,192 V 152" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-start="url(#arrowstart-ai)" marker-end="url(#arrowhead-ai)"/>
        <path d="M 736,536 H 832" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrowhead-ai)"/>
        <path d="M 1000,536 H 1096" fill="none" stroke="#7c3aed" stroke-width="1.2" stroke-dasharray="4,3" marker-end="url(#arrowhead-ai)"/>

        <!-- LABELS -->
        <rect x="98" y="186" width="52" height="12" rx="2" fill="#f5f5f5"/>
        <text x="124" y="196" fill="#4f5d75" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">1. CRAWL</text>

        <rect x="224" y="186" width="64" height="12" rx="2" fill="#f5f5f5"/>
        <text x="256" y="196" fill="#4f5d75" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">2. LOAD RAW</text>

        <rect x="398" y="186" width="60" height="12" rx="2" fill="#f5f5f5"/>
        <text x="428" y="196" fill="#4f5d75" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">3. EXTRACT</text>

        <rect x="316" y="186" width="64" height="12" rx="2" fill="#f5f5f5"/>
        <text x="348" y="196" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">4. SAVE JSON</text>

        <rect x="492" y="122" width="56" height="12" rx="2" fill="#f5f5f5"/>
        <text x="520" y="132" fill="#2e5aa8" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">5. SILVER</text>

        <rect x="596" y="230" width="48" height="12" rx="2" fill="#f5f5f5"/>
        <text x="620" y="240" fill="#2e5aa8" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">6. GOLD</text>

        <rect x="760" y="122" width="48" height="12" rx="2" fill="#f5f5f5"/>
        <text x="784" y="132" fill="#eb6c36" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">7. SYNC</text>

        <rect x="1018" y="122" width="60" height="12" rx="2" fill="#f5f5f5"/>
        <text x="1048" y="132" fill="#eb6c36" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">8. VECTORS</text>

        <rect x="756" y="454" width="64" height="12" rx="2" fill="#f5f5f5"/>
        <text x="788" y="464" fill="#2e5aa8" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">9. QUERY DB</text>

        <rect x="648" y="250" width="72" height="12" rx="2" fill="#f5f5f5"/>
        <text x="684" y="260" fill="#eb6c36" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">FIND EXPIRED</text>

        <rect x="478" y="282" width="84" height="12" rx="2" fill="#f5f5f5"/>
        <text x="520" y="292" fill="#eb6c36" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">UPDATE EXPIRED</text>

        <rect x="228" y="530" width="56" height="12" rx="2" fill="#f5f5f5"/>
        <text x="256" y="540" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">A. UPLOAD</text>

        <rect x="490" y="530" width="60" height="12" rx="2" fill="#f5f5f5"/>
        <text x="520" y="540" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">B. PROFILE</text>

        <rect x="804" y="474" width="92" height="12" rx="2" fill="#f5f5f5"/>
        <text x="850" y="484" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">C. HYBRID SEARCH</text>

        <rect x="760" y="530" width="48" height="12" rx="2" fill="#f5f5f5"/>
        <text x="784" y="540" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">D. TOP-K</text>

        <rect x="1022" y="530" width="52" height="12" rx="2" fill="#f5f5f5"/>
        <text x="1048" y="540" fill="#7c3aed" font-size="8" font-family="'Geist Mono', monospace" text-anchor="middle" letter-spacing="0.08em">E. RAG UI</text>


        <!-- NODES -->
        <!-- R1 -->
        <g transform="translate(40, 104)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2d3142" stroke-width="1.2"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">Web Crawlers</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">SeleniumBase, cffi</text>
        </g>
        <g transform="translate(304, 104)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2e5aa8" stroke-width="1.5"/>
          <text x="16" y="22" fill="#2e5aa8" font-size="12" font-weight="600">Bronze Layer</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">raw_*_jobs DuckDB</text>
        </g>
        <g transform="translate(568, 104)">
          <rect width="168" height="48" rx="6" fill="#eb6c36" stroke="#eb6c36" stroke-width="1.5"/>
          <text x="16" y="22" fill="#fff" font-size="12" font-weight="600">Silver Layer (dbt)</text>
          <text x="16" y="38" fill="rgba(255,255,255,0.8)" font-size="10">unified jobs &amp; TTL</text>
        </g>
        <g transform="translate(832, 104)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="rgba(45,49,66,0.2)"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">Qdrant Sync</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Upsert / Soft Delete</text>
        </g>
        <g transform="translate(1096, 104)">
          <rect width="168" height="48" rx="6" fill="#eb6c36" stroke="#eb6c36" stroke-width="1.5"/>
          <text x="16" y="22" fill="#fff" font-size="12" font-weight="600">Qdrant Local</text>
          <text x="16" y="38" fill="rgba(255,255,255,0.8)" font-size="10">Vector Embeddings</text>
        </g>

        <!-- R2 -->
        <g transform="translate(40, 232)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="rgba(45,49,66,0.2)"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">Landing Zone</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Raw Parquet Files</text>
        </g>
        <g transform="translate(304, 232)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#7c3aed" stroke-width="1.2"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">AI Extractor</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">GPT-4o (JSON)</text>
        </g>

        <!-- R3 -->
        <g transform="translate(304, 360)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2e5aa8" stroke-width="1.5"/>
          <text x="16" y="22" fill="#2e5aa8" font-size="12" font-weight="600">Gold Layer</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">dbt analytics marts</text>
        </g>
        <g transform="translate(568, 360)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#eb6c36" stroke-width="1.5" stroke-dasharray="4,3"/>
          <text x="16" y="22" fill="#eb6c36" font-size="12" font-weight="600">Purge Expired</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Manual / Ad-hoc script</text>
        </g>
        <g transform="translate(1096, 360)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2d3142" stroke-width="1.5"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">Tab 1: Analytics</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Streamlit Dashboard</text>
        </g>

        <!-- R4 -->
        <g transform="translate(40, 512)">
          <rect width="168" height="48" rx="6" fill="#f5f3ff" stroke="#7c3aed" stroke-width="1.2"/>
          <text x="16" y="22" fill="#7c3aed" font-size="12" font-weight="600">User CV Upload</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">PDF Document</text>
        </g>
        <g transform="translate(304, 512)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#7c3aed" stroke-width="1.2"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">CV Profiler</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">GPT-4o (YoE, Skills)</text>
        </g>
        <g transform="translate(568, 512)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2e5aa8" stroke-width="1.5"/>
          <text x="16" y="22" fill="#2e5aa8" font-size="12" font-weight="600">Hybrid Retriever</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Dense + Sparse Filter</text>
        </g>
        <g transform="translate(832, 512)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#7c3aed" stroke-width="1.2"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">HR Analyst</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">BGE Reranker &amp; GPT</text>
        </g>
        <g transform="translate(1096, 512)">
          <rect width="168" height="48" rx="6" fill="#fff" stroke="#2d3142" stroke-width="1.5"/>
          <text x="16" y="22" fill="#2d3142" font-size="12" font-weight="600">Tab 2: AI Coach</text>
          <text x="16" y="38" fill="#7a8399" font-size="10">Streamlit Conversational</text>
        </g>
      </svg>
    </div>
  </div>
</body>
</html>
"""

with open("docs/assets/architecture.html", "w") as f:
    f.write(html_content)
