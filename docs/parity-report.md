Result: **PASS** — every row and every column of every gold table matches the original.

- Engine: Databricks serverless, Spark 4.2.0
- Baseline: original pandas pipeline at commit `cdfcd29`
- Years processed by this run: `2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024`
- Generated at: 2026-11-18 01:06 (UTC)
- Float tolerance: 1e-09 (absolute); everything else compared exactly

| Table | Key | Rows (Spark) | Rows (original) | Only in Spark | Only in original | Columns compared | Cells differing | Status |
|---|---|---:|---:|---:|---:|---:|---:|:---:|
| `dim_uf` | uf_codigo | 27 | 27 | 0 | 0 | 2 | 0 | PASS |
| `fct_incidencia_tb` | ID_MN_RESI, NU_ANO | 41,074 | 41,074 | 0 | 0 | 6 | 0 | PASS |
| `fct_taxa_abandono` | ID_MN_RESI, NU_ANO | 39,710 | 39,710 | 0 | 0 | 4 | 0 | PASS |
| `fct_taxa_abandono_uf` | uf_codigo, ano | 270 | 270 | 0 | 0 | 5 | 0 | PASS |
| `fct_reconciliacao_sinan_sim` | uf_sigla, ano | 270 | 270 | 0 | 0 | 5 | 0 | PASS |

Aggregates (sum of each numeric column over the whole table):

| Table | Column | Spark | Original | Equal |
|---|---|---:|---:|:---:|
| `fct_incidencia_tb` | casos | 951,035 | 951,035 | yes |
| `fct_incidencia_tb` | populacao | 1,993,917,761 | 1,993,917,761 | yes |
| `fct_incidencia_tb` | incidencia_por_100k | 1,393,310.015543 | 1,393,310.015543 | yes |
| `fct_taxa_abandono` | n_casos_encerrados | 911,257 | 911,257 | yes |
| `fct_taxa_abandono` | n_abandono | 144,533 | 144,533 | yes |
| `fct_taxa_abandono` | taxa_abandono_pct | 371,301.804836 | 371,301.804836 | yes |
| `fct_taxa_abandono_uf` | n_casos_encerrados | 911,148 | 911,148 | yes |
| `fct_taxa_abandono_uf` | n_abandono | 144,520 | 144,520 | yes |
| `fct_taxa_abandono_uf` | taxa_abandono_pct | 3,907.110337 | 3,907.110337 | yes |
| `fct_reconciliacao_sinan_sim` | obitos_sim | 40,205 | 40,205 | yes |
| `fct_reconciliacao_sinan_sim` | obitos_sinan | 37,727 | 37,727 | yes |
| `fct_reconciliacao_sinan_sim` | diferenca_sim_menos_sinan | 11,138 | 11,138 | yes |
