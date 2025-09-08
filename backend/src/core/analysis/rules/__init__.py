# rules/__init__.py
from importlib import import_module
from pathlib import Path
from typing import List, Dict, Any, Callable
from core.models.recommendation import Recommendation
from core.models.lint_diagnose import LintDiagnose

RuleFunction = Callable[[str, Dict[str, Any], Dict[str, Any]], List[Recommendation]]

__all__ = []
_pkg_dir = Path(__file__).parent

for subdir in ['ast', 'custom']:
    subdir_path = _pkg_dir / subdir
    if subdir_path.exists() and subdir_path.is_dir():
        for py_file in subdir_path.glob("*.py"):
            module_name = py_file.stem
            if module_name.startswith("_"):
                continue
            try:
                import_module(f".{subdir}.{module_name}", __package__)
                __all__.append(f"{subdir}.{module_name}")
            except ImportError as e:
                print(f"Error importing {subdir}.{module_name}: {e}")

def _collect_rules() -> List[RuleFunction]:
    rules = []
    
    for module_name in __all__:
        try:
            full_module_name = f"{__package__}.{module_name}"
            module = import_module(full_module_name)
            
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if callable(attr) and attr_name.startswith("rule_"):
                    rules.append(attr)
                    
        except Exception as e:
            print(f"Error processing module {module_name}: {e}")
    
    return rules

all_rules = _collect_rules()

def analyze_with_rules(
    query: str, 
    plan: Dict[str, Any] = None,
    context: Dict[str, Any] = None
) -> List[Recommendation]:
    """
    Анализирует запрос с помощью всех загруженных правил
    """
    if context is None:
        context = {}
    if plan is None:
        plan = {}
        
    recommendations = []
    
    for rule_func in all_rules:
        try:
            rule_results = rule_func(query, plan, context)
            recommendations.extend(rule_results)
        except Exception as e:
            print(f"Error executing rule {rule_func.__name__}: {e}")
    
    return recommendations

__all__.append('analyze_with_rules')
__all__.append('RuleFunction')
