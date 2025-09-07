# rules/__init__.py
from importlib import import_module
from pathlib import Path

__all__ = []

_pkg_dir = Path(__file__).parent
for _py_file in _pkg_dir.glob("*.py"):
    _name = _py_file.stem
    if _name.startswith("_"):
        continue
    import_module(f".{_name}", __package__)
    __all__.append(_name)
