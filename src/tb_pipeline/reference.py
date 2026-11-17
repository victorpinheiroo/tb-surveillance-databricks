"""Reference constants carried over unchanged from the original pandas pipeline.

Source of truth: github.com/victorpinheiroo/tb-surveillance-datasus at commit
cdfcd297fed92ade2dba524fcb807cfc8508aa26 (the parity baseline).
"""

# Baseline commit of the original repository: bronze input and gold output
# used by the parity report both come from this exact commit. The original
# repo re-runs monthly and rewrites its data, so the baseline must be pinned.
BASELINE_COMMIT = "cdfcd297fed92ade2dba524fcb807cfc8508aa26"

# IBGE state code <-> abbreviation <-> name (27 states, static).
UF_MAPPING = [
    ("11", "RO", "Rondônia"), ("12", "AC", "Acre"), ("13", "AM", "Amazonas"),
    ("14", "RR", "Roraima"), ("15", "PA", "Pará"), ("16", "AP", "Amapá"),
    ("17", "TO", "Tocantins"), ("21", "MA", "Maranhão"), ("22", "PI", "Piauí"),
    ("23", "CE", "Ceará"), ("24", "RN", "Rio Grande do Norte"), ("25", "PB", "Paraíba"),
    ("26", "PE", "Pernambuco"), ("27", "AL", "Alagoas"), ("28", "SE", "Sergipe"),
    ("29", "BA", "Bahia"), ("31", "MG", "Minas Gerais"), ("32", "ES", "Espírito Santo"),
    ("33", "RJ", "Rio de Janeiro"), ("35", "SP", "São Paulo"), ("41", "PR", "Paraná"),
    ("42", "SC", "Santa Catarina"), ("43", "RS", "Rio Grande do Sul"),
    ("50", "MS", "Mato Grosso do Sul"), ("51", "MT", "Mato Grosso"),
    ("52", "GO", "Goiás"), ("53", "DF", "Distrito Federal"),
]

# SINAN-TB treatment outcome dictionary (SITUA_ENCE), confirmed against the
# Ministry of Health data dictionary in the original project. Codes arrive
# zero-padded in some years ("03"), so they are normalised before lookup.
SITUA_ENCE_MAP = {
    "1": "Cura",
    "2": "Abandono",
    "3": "Óbito por Tuberculose",
    "4": "Óbito por outras causas",
    "5": "Transferência",
    "6": "Mudança de Diagnóstico",
    "7": "TB-DR",
    "8": "Mudança de Esquema",
    "9": "Falência",
    "10": "Abandono Primário",
}

OBITO_TB = "Óbito por Tuberculose"
ABANDONO_CATEGORIAS = ("Abandono", "Abandono Primário")

# Columns that only exist in older SINAN-TB files and are dropped in silver.
SINAN_LEGACY_COLUMNS = ("AGRAVOUTDE", "EXTRAPUL_O", "OUTRAS_DES")

# SINAN-TB date columns, stored as yyyyMMdd strings in the source.
SINAN_DATE_COLUMNS = (
    "DT_NOTIFIC", "DT_DIAG", "DT_DIGITA", "DT_TRANSUS", "DT_TRANSDM",
    "DT_TRANSSM", "DT_TRANSRM", "DT_TRANSRS", "DT_TRANSSE", "DT_INIC_TR",
    "DT_NOTI_AT", "DT_MUDANCA", "DT_ENCERRA",
)

# Year from which SIM figures are still being consolidated at the source.
SIM_PROVISIONAL_FROM_YEAR = 2024
