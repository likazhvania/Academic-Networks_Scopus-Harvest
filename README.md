# Academic Networks - Scopus Harvest

# Academic Networks – Scopus Harvest  
*Full‑period metadata dump (2000 – 2024) for collaboration‑network research*

---

## 1&nbsp;·&nbsp;Project purpose
This repository contains an automated harvester that downloads **all journal‑article
records** (`DOCTYPE=ar`) from the Scopus API for the publication years **2000 → 2024**.
The data will later feed:

* global affiliation & co‑authorship networks  
* longitudinal productivity / interdisciplinarity studies  
* subject‑area growth analyses and other scientometrics

---

## 2&nbsp;·&nbsp;Key design points

| Constraint | Solution |
|------------|----------|
| Weekly quota **100 000 requests** (25 records each) | Split into **three runs** (Mon 40 k · Tue 30 k · Thu 30 k). |
| Cursor expires after ≈ 7 days | Run ≥ 2 times per week and persist `cursor_state.json`. |
| Rate limit **≤ 9 requests/s** | Token‑bucket or `ratelimit` decorator. |
| Memory safety | Flush **2 000 requests / chunk** (≈ 50 000 docs) to disk. |
| Data fidelity | Save raw compressed **JSONL** + optional CSV flatten later. |
| Secrets / data privacy | `.env` for API key; data folders ignored via `.gitignore`. |

---

## 3&nbsp;·&nbsp;Repository contents
