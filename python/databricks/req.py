# =============================================================
#  Rechargement tables agrégées trafics (jour / semaine / mois)
#  3 sources -> 3 cibles, pas de filtre temporel
# =============================================================
from pyspark.sql import DataFrame, functions as F
from functools import reduce
from operator import add

# =============================================================
# 1. CONFIGURATION
# =============================================================
# --- Sources (placeholders à remplacer) ---
SRC_JOUR    = "ppd_dd_kairos_int.03_gold.SRC_JOUR"
SRC_SEMAINE = "ppd_dd_kairos_int.03_gold.SRC_SEMAINE"
SRC_MOIS    = "ppd_dd_kairos_int.03_gold.SRC_MOIS"

# --- Cibles ---
DST_JOUR    = "ppd_dd_kairos_int.03_gold.g_mdp_trafics_agrege_jour"
DST_SEMAINE = "ppd_dd_kairos_int.03_gold.g_mdp_trafics_agrege_semaine"
DST_MOIS    = "ppd_dd_kairos_int.03_gold.g_mdp_trafics_agrege_mois"

# --- Mode d'écriture : "FULL" (overwrite) ou "MERGE" (upsert) ---
MODE = "FULL"

# --- Mapping granularité -> (source, cible, colonne_date_source) ---
GRANULARITES = {
    "jour":    {"src": SRC_JOUR,    "dst": DST_JOUR,    "col_date": "da_comptage"},
    "semaine": {"src": SRC_SEMAINE, "dst": DST_SEMAINE, "col_date": "co_semaine_comptage"},
    "mois":    {"src": SRC_MOIS,    "dst": DST_MOIS,    "col_date": "co_mois_comptage"},
}

# --- Règles métier ---
SITES_DIST = ["PDC1", "PDC2", "PPDC"]
SITES_PIC  = ["PIC", "CTC"]
TOUS_SITES = SITES_DIST + SITES_PIC

COMPTAGES_AUTORISES = [
    "TI_COL_MENAGE", "TI_COL_CEDEX",
    "OR4PM", "OR4PX",
    "TRSP1", "IMPJ", "TLOP1",
    "VQQP0", "2QNP1", "2QQP1",
    "PPLP0", "1POP0", "VPIP0",
]

TRAFIC_COLS = [
    "trafic_oo", "presse_mecanisee", "presse_locale_declarative",
    "presse_viapost_hors_meca", "trafic_os", "trafic_ip",
    "trafic_colis", "trafic_ppi",
]

MERGE_KEYS = ["da_comptage", "co_regate", "type_site"]


# =============================================================
# 2. FONCTIONS
# =============================================================
def construire_agregat(src_table: str, col_date_source: str) -> DataFrame:
    """
    Lit la table source, applique les règles métier et agrège.
    La colonne date source est renommée en 'da_comptage' (STRING) dans la cible.
    """
    df = (spark.table(src_table)
            .filter(F.col("lb_type_entite_regate_court").isin(TOUS_SITES))
            .filter(
                F.col("co_process").isin("VT", "DT")
                | F.col("co_comptage").isin(COMPTAGES_AUTORISES)
            ))

    is_dist = F.col("lb_type_entite_regate_court").isin(SITES_DIST)
    is_pic  = F.col("lb_type_entite_regate_court").isin(SITES_PIC)

    def regle(cond):
        return F.when(cond, F.col("trafic_reel")).otherwise(F.lit(0))

    df_calc = df.select(
        F.col(col_date_source).cast("string").alias("da_comptage"),
        F.col("co_regate"),
        F.col("lb_type_entite_regate_court").alias("type_site"),
        regle(
            (is_dist & F.col("co_comptage").isin("TI_COL_MENAGE", "TI_COL_CEDEX"))
            | (is_dist & F.col("co_comptage").isin("OR4PM", "OR4PX") & (F.col("co_type_objet") == "R"))
            | (is_pic  & F.col("co_process").isin("VT", "DT")        & (F.col("co_type_objet") == "R"))
        ).alias("trafic_oo"),
        regle(
            (is_dist & F.col("co_comptage").isin("OR4PM", "OR4PX") & (F.col("co_type_objet") == "P"))
            | (is_pic  & F.col("co_process").isin("VT", "DT")       & (F.col("co_type_objet") == "P"))
        ).alias("presse_mecanisee"),
        regle(is_dist & (F.col("co_comptage") == "PPLP0")).alias("presse_locale_declarative"),
        regle(is_dist & F.col("co_comptage").isin("1POP0", "VPIP0")).alias("presse_viapost_hors_meca"),
        regle(is_dist & (F.col("co_comptage") == "TRSP1")).alias("trafic_os"),
        regle(is_dist & (F.col("co_comptage") == "IMPJ")).alias("trafic_ip"),
        regle(is_dist & (F.col("co_comptage") == "TLOP1")).alias("trafic_colis"),
        regle(is_dist & F.col("co_comptage").isin("VQQP0", "2QNP1", "2QQP1")).alias("trafic_ppi"),
    )

    return (df_calc
        .groupBy("da_comptage", "co_regate", "type_site")
        .agg(*[F.sum(c).alias(c) for c in TRAFIC_COLS])
        .withColumn("trafic_total", reduce(add, [F.col(c) for c in TRAFIC_COLS]))
        .select("da_comptage", "co_regate", "type_site", *TRAFIC_COLS, "trafic_total"))


def ecrire(df: DataFrame, dst_table: str, mode: str):
    if mode == "FULL":
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(dst_table))
        return

    if mode == "MERGE":
        if not spark.catalog.tableExists(dst_table):
            df.write.format("delta").saveAsTable(dst_table)
            return
        df.createOrReplaceTempView("_src_incr")
        on_clause  = " AND ".join([f"tgt.{k} = src.{k}" for k in MERGE_KEYS])
        update_set = ", ".join([f"{c} = src.{c}" for c in TRAFIC_COLS + ["trafic_total"]])
        all_cols   = MERGE_KEYS + TRAFIC_COLS + ["trafic_total"]
        insert_col = ", ".join(all_cols)
        insert_val = ", ".join([f"src.{c}" for c in all_cols])
        spark.sql(f"""
            MERGE INTO {dst_table} tgt
            USING _src_incr src
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_col}) VALUES ({insert_val})
        """)
        return

    raise ValueError(f"MODE inconnu : {mode}")


# =============================================================
# 3. EXÉCUTION
# =============================================================
def run():
    for gran, cfg in GRANULARITES.items():
        print(f"=== {gran.upper()} | {cfg['src']} -> {cfg['dst']} ===")
        df = construire_agregat(cfg["src"], cfg["col_date"])
        ecrire(df, cfg["dst"], MODE)
        print(f"    OK")

run()