Result: **PARTIAL RUN** — only part of the year window was processed, so differences against the full-window baseline are expected and do not fail the job.

- Engine: Databricks serverless, Spark 4.2.0
- Baseline: original pandas pipeline at commit `cdfcd29`
- Years processed by this run: `2015`
- Generated at: 2026-11-18 00:45 (UTC)
- Float tolerance: 1e-09 (absolute); everything else compared exactly

| Table | Key | Rows (Spark) | Rows (original) | Only in Spark | Only in original | Columns compared | Cells differing | Status |
|---|---|---:|---:|---:|---:|---:|---:|:---:|
| `dim_uf` | uf_codigo | 27 | 27 | 0 | 0 | 2 | 0 | PASS |
| `fct_incidencia_tb` | ID_MN_RESI, NU_ANO | 4,882 | 41,074 | 0 | 36,192 | 6 | 1,881 | FAIL |
| `fct_taxa_abandono` | ID_MN_RESI, NU_ANO | 4,740 | 39,710 | 0 | 34,970 | 4 | 2,139 | FAIL |
| `fct_taxa_abandono_uf` | uf_codigo, ano | 98 | 270 | 0 | 172 | 5 | 216 | FAIL |
| `fct_reconciliacao_sinan_sim` | uf_sigla, ano | 27 | 270 | 0 | 243 | 5 | 2 | FAIL |

Aggregates (sum of each numeric column over the whole table):

| Table | Column | Spark | Original | Equal |
|---|---|---:|---:|:---:|
| `fct_incidencia_tb` | casos | 85,368 | 951,035 | NO |
| `fct_incidencia_tb` | populacao | 461,452,480 | 1,993,917,761 | NO |
| `fct_incidencia_tb` | incidencia_por_100k | 123,039.142754 | 1,393,310.015543 | NO |
| `fct_taxa_abandono` | n_casos_encerrados | 82,051 | 911,257 | NO |
| `fct_taxa_abandono` | n_abandono | 10,719 | 144,533 | NO |
| `fct_taxa_abandono` | taxa_abandono_pct | 44,264.301915 | 371,301.804836 | NO |
| `fct_taxa_abandono_uf` | n_casos_encerrados | 81,998 | 911,148 | NO |
| `fct_taxa_abandono_uf` | n_abandono | 10,712 | 144,520 | NO |
| `fct_taxa_abandono_uf` | taxa_abandono_pct | 1,971.089452 | 3,907.110337 | NO |
| `fct_reconciliacao_sinan_sim` | obitos_sim | 4,373 | 40,205 | NO |
| `fct_reconciliacao_sinan_sim` | obitos_sinan | 2,970 | 37,727 | NO |
| `fct_reconciliacao_sinan_sim` | diferenca_sim_menos_sinan | 1,583 | 11,138 | NO |

Columns with differences:

- `fct_incidencia_tb.casos`: 941 row(s)
- `fct_incidencia_tb.incidencia_por_100k`: 940 row(s)
- `fct_taxa_abandono.n_casos_encerrados`: 909 row(s)
- `fct_taxa_abandono.n_abandono`: 588 row(s)
- `fct_taxa_abandono.taxa_abandono_pct`: 642 row(s)
- `fct_taxa_abandono_uf.n_casos_encerrados`: 72 row(s)
- `fct_taxa_abandono_uf.n_abandono`: 72 row(s)
- `fct_taxa_abandono_uf.taxa_abandono_pct`: 72 row(s)
- `fct_reconciliacao_sinan_sim.obitos_sinan`: 1 row(s)
- `fct_reconciliacao_sinan_sim.diferenca_sim_menos_sinan`: 1 row(s)
