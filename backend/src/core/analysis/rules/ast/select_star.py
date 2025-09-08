import sqlparse
from typing import Dict, List, Any
from core.models.recommendation import Recommendation
from core.models.lint_diagnose import LintDiagnose


def rule_select_star(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[Recommendation]:
    """
    Обнаруживает использование SELECT * в запросах
    """
    recommendations = []
    
    try:
        parsed = sqlparse.parse(query)
        
        for statement in parsed:
            for token in statement.tokens:
                if (hasattr(token, 'ttype') and 
                    token.ttype is sqlparse.tokens.Wildcard and 
                    str(token).strip() == "*"):
                    
                    line = 1
                    col = 1
                    
                    if hasattr(statement, 'token_index'):
                        token_index = statement.token_index(token)
                        if token_index != -1:
                            col = query.find('*', max(0, query.find('select') + 6))
                            if col == -1:
                                col = 1
                    
                    diagnose = LintDiagnose(
                        line=line,
                        col=col,
                        severity="MEDIUM",
                        message="Обнаружено использование SELECT * в запросе"
                    )
                    
                    recommendations.append(
                        Recommendation(
                            diagnose=diagnose,
                            recommendation="Явно укажите необходимые колонки вместо использования *. Это улучшит производительность и сделает запрос более понятным."
                        )
                    )
                    break  
    
    except Exception as e:
        print(f"Error in select_star rule: {e}")
    
    return recommendations
