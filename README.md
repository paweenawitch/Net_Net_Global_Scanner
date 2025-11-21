# **Global Net-Net Stock Scanner (Open Source)**

A multidisciplinary project rooted in the tradition of Benjamin Graham and the rationalism of Graham-and-Doddsville.

This is a fully automated global NCAV (Net Current Asset Value) scanner covering the U.S., Japan, and Hong Kong. I built it because manually searching for net-nets across markets was slow, fragmented, and error-prone. I wanted a transparent and reproducible system — something I wish I had when I first started studying deep-value investing.

It is open-source by design. If this helps even one investor, researcher, or student the way it helps me, that is enough. Graham democratized value investing and gave ordinary people a framework for moderate prosperity. This project is simply a continuation of that spirit.

---

## **What This Project Does**

The scanner builds a global ticker universe, evaluates every company trading below its net current asset value, fetches fundamentals, normalizes currencies, checks solvency and insider behavior, and outputs structured valuation files.

The goal is to answer this question:

**Which companies in the world are trading below their net liquidation value?**

### **Capabilities at a Glance**

* Unified global ticker universe (US, JP, HK — extendable to any market)
* Fundamentals fetched and cached from Yahoo Finance + SEC EDGAR
* Full NCAV and price-to-NCAV computation
* Graham’s 2/3 NCAV margin-of-safety flag
* Liquidity, solvency, dilution, and insider-activity evaluation
* Clean CSV/JSON valuation outputs for research, analysis, or reporting

---

## **Key Features**

### **NCAV & Valuation**

* NCAV = Current Assets – Total Liabilities
* NCAV/share in native and reporting currency
* Price < NCAV/share flag
* Optional 2/3 NCAV margin-of-safety flag
* Price-to-NCAV ratio

### **Financial Quality**

* Current ratio
* Debt-to-equity
* NCAV trends (QoQ, HoH, YoY)
* Stale or outdated filings detection

### **Capital Discipline**

* Share count changes over 1y and 3y
* Detection of dilution or buyback behavior

### **Insider Activity**

* Buy / Sell / Net Buy / Net Sell within last 6 months
* Last insider transaction date
* Insider % held (best-effort from Yahoo)

### **FX Handling**

* Normalized currency resolver
* USD-cross conversion
* Graceful fallback behavior

### **Extensible Architecture**

Add a new market by supplying:

* a universe CSV
* a market source adapter
* a currency entry in the FX table

The architecture is intentionally simple so future contributors, students, or researchers can extend it easily.

---

## **Quickstart (End-to-End Pipeline)**

**1. Clone**

```bash
git clone https://github.com/paweenawitch/global-net-net-scanner.git
cd global-net-net-scanner
```

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

**3. Build the global ticker universe**

```bash
python -m application.cli.build_universe --root .
```

Output: `data/tickers/global_full.csv`

**4. Build the NCAV shortlist**

```bash
python -m application.cli.main_build_shortlist --tickers_csv data/tickers/global_full.csv
```

Output: `data/tickers/ncav_shortlist.csv`

**5. Fetch and refresh fundamentals**

```bash
python -m application.cli.main_fetch_full_cache --verbose
```
You may skip this stage if you believe there is no need to update full cache after the first run.
Output: 'cache/sec_core/' and 'cache/sec_insider/

**6. Run the screening engine**

```bash
python tools/screening_engine.py
```

Outputs include:

* `latest_flags.csv`
* `latest_flags.json`
* internal debug reports

The entire pipeline is deterministic and reproducible.

---

## **Sample Output**

A simplified example of what the system produces:

| ticker  | country | ncav_ps | last_price | price_to_ncavps | insider_signal | green_flags        | red_flags            |
| ------- | ------- | ------- | ---------- | --------------- | -------------- | ------------------ | -------------------- |
| ACON.US | US      | 0.455   | 0.175      | 0.38            | None           | Trading =< 2/3 NCAV | None                 |
| ACRV.US | US      | 3.102   | 1.320      | 0.42            | None           | Trading =< 2/3 NCAV | None                 |
| AGMH.US | HK      | 1.090   | 0.450      | 0.41            | None           | Trading =< 2/3 NCAV | None                 |
| 6771.JP | JP      | 1310    | 598        | 0.46            | None           | Trading =< 2/3 NCAV, Current ratio >= 2 | None |
| 1522.HK | HK      | 0.683   | 0.350      | 0.41            | None           | Trading =< 2/3 NCAV, Current ratio >= 2, NCAV stable YoY or improving | None |

Full reports live under `public/reports/`.

---

## **Architecture Overview**

The codebase follows a straightforward three-layer structure:

```
domain/
    models/        # Company, FinancialSnapshot, ValuationResult
    services/      # NCAV math, FX logic, flags, trends, insider logic

application/
    cli/           # user-facing commands and orchestrators
    services/      # shortlist builder, fetch orchestrators, screening service, market registry

infrastructure/
    sources/       # adapters: SEC, Yahoo Finance, JPX, HKEX
    repositories/  # universe writer, shortlist repo, FS loaders
    reporting/     # CSV/JSON writers
    fx/            # exchange-rate providers
    config/        # path designation
    runners/       # script runner
```
---

## **Contributions**

Contributions are welcome, especially around:

* additional global exchanges
* FX improvements
* performance and caching
* quality and trend metrics
* documentation and examples
* test suites
* academic research extensions

---

## **License**

MIT — free for personal, academic, and commercial use.
