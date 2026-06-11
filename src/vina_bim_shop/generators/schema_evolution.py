from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from vina_bim_shop.generators.config import GeneratorConfig


def category_attributes(rng: np.random.Generator, category: str, subcategory: str) -> str:
    if category == "FMCG":
        payload = {
            "pack_size": rng.choice(["single", "bundle_3", "family_pack"]),
            "shelf_life_days": int(rng.integers(90, 720)),
            "variant": rng.choice(["standard", "sensitive", "premium"]),
        }
    elif category == "ELHA":
        payload = {
            "warranty_months": int(rng.choice([6, 12, 18, 24])),
            "energy_rating": rng.choice(["A", "A+", "A++", "not_applicable"]),
            "spec_tier": rng.choice(["entry", "mid", "flagship"]),
        }
    elif category == "Fashion":
        payload = {
            "size_family": rng.choice(["alpha", "numeric", "free_size"]),
            "material": rng.choice(["cotton", "polyester", "denim", "synthetic_leather"]),
            "style": rng.choice(["casual", "office", "streetwear"]),
        }
    else:
        payload = {
            "room_type": rng.choice(["kitchen", "bedroom", "living_room", "bathroom"]),
            "assembly_required": bool(rng.random() < 0.35),
            "material": rng.choice(["wood", "steel", "ceramic", "fabric"]),
        }
    payload["subcategory"] = subcategory
    return json.dumps(payload, sort_keys=True)


def schema_evolution_cutoff(config: GeneratorConfig) -> pd.Timestamp:
    end_ts: pd.Timestamp = pd.Timestamp(config.end_date).normalize() + pd.Timedelta(hours=23, minutes=59)
    return end_ts - pd.Timedelta(days=int(config.history_days * (1 - float(config.quality["schema_evolution_cutoff_ratio"]))))


__all__ = ["category_attributes", "schema_evolution_cutoff"]
