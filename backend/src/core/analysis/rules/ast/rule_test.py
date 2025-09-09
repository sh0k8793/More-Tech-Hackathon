from typing import Any, Dict, List
from core.models.lint_diagnose import LintDiagnose


def rule_test(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    lint_diagnoses = [
        LintDiagnose(
            line=1,
            col=1,
            severity="MEDIUM",
            message="Message",
            recommendation="recommendation"
        ),
        LintDiagnose(
            line=1,
            col=1,
            severity="MEDIUM",
            message="Message213",
            recommendation="recommendation"
        )
    ]
                    
    return lint_diagnoses, query
