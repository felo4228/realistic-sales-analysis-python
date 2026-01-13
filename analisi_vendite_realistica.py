from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd




PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

RANDOM_SEED = 42




def ensure_dirs() -> None:
    """Crea las carpetas del proyecto si no existen."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def memory_mb(df: pd.DataFrame) -> float:
    """Devuelve el uso de memoria del DataFrame en MB."""
    return df.memory_usage(deep=True).sum() / (1024 ** 2)


# =========================
# Parte 1 - datasets
# =========================

def create_prodotti_json(n_prodotti: int = 20) -> Path:
    
    rng = np.random.default_rng(RANDOM_SEED)

    categorie = ["A", "B", "C", "D"]
    fornitori = ["SupplierOne", "SupplierTwo", "SupplierThree", "SupplierFour"]

    prodotti = []
    for pid in range(1, n_prodotti + 1):
        prodotti.append(
            {
                "ProdottoID": pid,
                "NomeProdotto": f"Prodotto_{pid:02d}",
                "Categoria": rng.choice(categorie),
                "Fornitore": rng.choice(fornitori),
                "Prezzo": round(float(rng.uniform(5, 300)), 2),
            }
        )

    out_path = RAW_DIR / "prodotti.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(prodotti, f, ensure_ascii=False, indent=2)

    return out_path


def create_clienti_csv(n_clienti: int = 5_000) -> Path:
   
    rng = np.random.default_rng(RANDOM_SEED)

    regioni = ["Nord", "Centro", "Sud", "Isole"]
    segmenti = ["Standard", "Premium", "Business"]

    df_clienti = pd.DataFrame(
        {
            "ClienteID": np.arange(1, n_clienti + 1, dtype=np.int32),
            "Regione": rng.choice(regioni, size=n_clienti),
            "Segmento": rng.choice(segmenti, size=n_clienti, p=[0.7, 0.2, 0.1]),
        }
    )

    out_path = RAW_DIR / "clienti.csv"
    df_clienti.to_csv(out_path, index=False)
    return out_path


def create_ordini_csv(n_ordini: int = 100_000, n_clienti: int = 5_000, n_prodotti: int = 20) -> Path:
   
    rng = np.random.default_rng(RANDOM_SEED)

    start_date = datetime(2024, 1, 1)
    days_range = 365

    df_ordini = pd.DataFrame(
        {
            "OrdineID": np.arange(1, n_ordini + 1, dtype=np.int32),
            "ClienteID": rng.integers(1, n_clienti + 1, size=n_ordini, dtype=np.int32),
            "ProdottoID": rng.integers(1, n_prodotti + 1, size=n_ordini, dtype=np.int16),
            "Quantità": rng.integers(1, 11, size=n_ordini, dtype=np.int16),
            "DataOrdine": [
                (start_date + timedelta(days=int(x))).strftime("%Y-%m-%d")
                for x in rng.integers(0, days_range, size=n_ordini)
            ],
        }
    )

    out_path = RAW_DIR / "ordini.csv"
    df_ordini.to_csv(out_path, index=False)
    return out_path


# =========================
# Parte 2 - DataFrame unificato
# =========================

def load_prodotti(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8") as f:
        prodotti = json.load(f)
    return pd.DataFrame(prodotti)


def build_unified_dataframe(ordini_path: Path, prodotti_path: Path, clienti_path: Path) -> pd.DataFrame:
    
    df_ordini = pd.read_csv(ordini_path)
    df_prodotti = load_prodotti(prodotti_path)
    df_clienti = pd.read_csv(clienti_path)

    df = df_ordini.merge(df_prodotti, on="ProdottoID", how="left")
    df = df.merge(df_clienti, on="ClienteID", how="left")

    return df


# =========================
# Parte 3 - Ottimizzare l’uso della memoria (dtypes)
# =========================

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    
    df_opt = df.copy()

    int_cols = ["OrdineID", "ClienteID", "ProdottoID", "Quantità"]
    for col in int_cols:
        df_opt[col] = pd.to_numeric(df_opt[col], downcast="integer")

    if "Prezzo" in df_opt.columns:
        df_opt["Prezzo"] = pd.to_numeric(df_opt["Prezzo"], downcast="float")

    cat_cols = ["Categoria", "Fornitore", "Regione", "Segmento", "NomeProdotto"]
    for col in cat_cols:
        if col in df_opt.columns:
            df_opt[col] = df_opt[col].astype("category")

    if "DataOrdine" in df_opt.columns:
        df_opt["DataOrdine"] = pd.to_datetime(df_opt["DataOrdine"], errors="coerce")

    return df_opt


# =========================
# Parte 4 - Creare colonne e filtra i dati
# =========================

def add_valore_totale(df: pd.DataFrame) -> pd.DataFrame:
    
    df2 = df.copy()
    df2["ValoreTotale"] = df2["Prezzo"] * df2["Quantità"]
    return df2


def filter_orders(df: pd.DataFrame) -> pd.DataFrame:
    
    df_filtered = df.loc[
        (df["ValoreTotale"] > 100) & (df["Segmento"] == "Premium")
    ].copy()
    return df_filtered



# =========================
# Main (pipeline completo)
# =========================

def main() -> None:
    ensure_dirs()

    # ---- Parte 1: generazione datasets
    prodotti_path = create_prodotti_json(n_prodotti=20)
    clienti_path = create_clienti_csv(n_clienti=5_000)
    ordini_path = create_ordini_csv(n_ordini=100_000, n_clienti=5_000, n_prodotti=20)

    print("Datasets generados en:", RAW_DIR)

    # ---- Parte 2: unificare
    df = build_unified_dataframe(ordini_path, prodotti_path, clienti_path)
    print("\nDF unificado (antes de optimizar)")
    print(df.head())
    print(f"Memoria antes: {memory_mb(df):.2f} MB")

    # ---- Parte 3: ottimizare dtypes
    df_opt = optimize_dtypes(df)
    print("\nDF unificado (después de optimizar)")
    print(df_opt.head())
    print(f"Memoria después: {memory_mb(df_opt):.2f} MB")

    unified_path = PROCESSED_DIR / "unified_optimized.parquet"
    df_opt.to_parquet(unified_path, index=False)
    print("\nGuardado:", unified_path)

    # ---- Parte 4: colonne e filtra i dati
    df_calc = add_valore_totale(df_opt)
    df_filtered = filter_orders(df_calc)

    filtered_path = PROCESSED_DIR / "filtered_orders.parquet"
    df_filtered.to_parquet(filtered_path, index=False)

    print("\nResumen:")
    print("Filas totales:", len(df_calc))
    print("Filas filtradas:", len(df_filtered))
    print("Guardado:", filtered_path)

    

if __name__ == "__main__":
    main()
