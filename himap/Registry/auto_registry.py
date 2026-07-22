"""
Auto-registration for generated towns — HiMap v3.0

Scans Towns/towns/ (populated by generate_towns.py), imports each
module, finds the TownBase subclass it defines, instantiates it, and
registers it under a slugified key derived from the town's name.

Key derivation deliberately mirrors the existing hand-picked convention
("lowercase, hyphenated, no country suffix" — see TownRegistry.py's
docstring): 'Homa Bay' -> 'homa-bay'. It's derived from the instance's
.name field (the original, space-containing town name) rather than
reversing the PascalCase classname/filename, which would be lossy.

Lives in himap/Registry/ (top-level, sibling of Towns/) — so paths and
imports go up to himap/ first, then back down into Towns/.
"""

import importlib
import inspect
import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple

from ..Towns.TownBase import TownBase
from ..Towns.TownRegistry import TownRegistry

logger = logging.getLogger(__name__)

_TOWNS_PACKAGE = "himap.Towns.towns"
# __file__ = himap/Registry/auto_registry.py
#   .parent      -> himap/Registry
#   .parent.parent -> himap
# then back down into Towns/towns
_TOWNS_DIR = Path(__file__).resolve().parent.parent / "Towns" / "towns"


def _slugify(name: str) -> str:
    """'Homa Bay' -> 'homa-bay'. "Murang'a" -> 'murang-a'."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def discover_towns(towns_dir: Optional[Path] = None) -> List[Tuple[str, TownBase]]:
    """
    Import every module under Towns/towns/, find the TownBase subclass
    it defines (generate_towns.py writes exactly one per file), and
    instantiate it.

    Returns (slug_key, TownBase instance) pairs. Does not register
    anything — see register_generated(). Import or instantiation
    failures are logged and skipped rather than raised, since one bad
    generated file shouldn't take down every other town.
    """
    towns_dir = towns_dir or _TOWNS_DIR
    discovered: List[Tuple[str, TownBase]] = []

    if not towns_dir.exists():
        logger.warning(f"Towns directory not found: {towns_dir} — nothing to auto-register")
        return discovered

    for path in sorted(towns_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue

        module_name = f"{_TOWNS_PACKAGE}.{path.stem}"
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            logger.error(f"Failed to import town module '{module_name}': {e}")
            continue

        town_classes = [
            obj for _, obj in inspect.getmembers(module, inspect.isclass)
            if issubclass(obj, TownBase)
            and obj is not TownBase
            and obj.__module__ == module_name
        ]

        if not town_classes:
            logger.warning(f"No TownBase subclass found in '{module_name}' — skipping")
            continue
        if len(town_classes) > 1:
            logger.warning(
                f"Multiple TownBase subclasses in '{module_name}' "
                f"({[c.__name__ for c in town_classes]}) — using the first"
            )

        try:
            instance = town_classes[0]()
        except Exception as e:
            logger.error(f"Failed to instantiate town from '{module_name}': {e}")
            continue

        discovered.append((_slugify(instance.name), instance))

    return discovered


def register_generated(
    registry: TownRegistry,
    towns_dir: Optional[Path] = None,
    on_conflict: str = "skip",
) -> List[str]:
    """
    Discover and register every generated town.

    on_conflict:
        "skip"  (default) — a key that's already registered (almost
                always because curated_towns.py registered it first —
                nairobi/mombasa/kisumu collide by design) is logged and
                left alone rather than raising.
        "raise" — surface TownRegistry's ValueError instead.

    Returns the list of newly registered keys (collisions are not
    included, since nothing new was registered for them).
    """
    registered: List[str] = []

    for key, town in discover_towns(towns_dir):
        if key in registry:
            if on_conflict == "raise":
                raise ValueError(f"Town '{key}' already registered.")
            logger.info(f"Town '{key}' already registered (curated) — skipping generated copy")
            continue

        try:
            registry.register(key, town)
            registered.append(key)
        except ValueError as e:
            # town.validate() failed inside register() — e.g. bbox > 10°
            logger.warning(f"Could not register generated town '{key}': {e}")

    logger.info(f"Auto-registered {len(registered)} generated towns: {registered}")
    return registered
