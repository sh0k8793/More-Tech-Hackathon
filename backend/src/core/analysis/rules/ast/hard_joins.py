# backend/src/core/analysis/rules/ast/join_optimizer.py
from typing import Any, Dict, List, Tuple
import re
from core.models.lint_diagnose import LintDiagnose

def rule_join_optimizer(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> Tuple[List[LintDiagnose], str]:
    """Обнаруживает и оптимизирует сложные JOIN"""
    recommendations = []
    optimized_query = query
    
    try:
        # 1. Cartesian JOIN (CROSS JOIN или JOIN без условия)
        if 'CROSS JOIN' in query.upper():
            cross_match = re.search(r'CROSS JOIN\s+(\w+)', query, re.IGNORECASE)
            if cross_match:
                diagnose = LintDiagnose(
                    line=1,
                    col=cross_match.start() + 1,
                    severity="HIGH",
                    message="CROSS JOIN может быть очень ресурсоемким",
                    recommendation="Убедитесь, что CROSS JOIN действительно необходим, или замените на INNER JOIN с условием"
                )
                recommendations.append(diagnose)
        
        # 2. JOIN без условия (неявный Cartesian)
        join_without_on = re.search(r'JOIN\s+\w+(?:\s+\w+)?(?:\s+WHERE|\s+ORDER BY|\s+GROUP BY|$|;)', query, re.IGNORECASE)
        if join_without_on and ' ON ' not in query.upper() and ' USING ' not in query.upper():
            diagnose = LintDiagnose(
                line=1,
                col=join_without_on.start() + 1,
                severity="HIGH",
                message="JOIN без условия может создавать Cartesian product",
                recommendation="Добавьте условие ON или USING для JOIN"
            )
            recommendations.append(diagnose)
        
        # 3. Слишком много JOIN в одном запросе
        join_count = query.upper().count(' JOIN ')
        if join_count > 3:
            diagnose = LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"Запрос содержит {join_count} JOIN операций, что может быть ресурсоемким",
                recommendation="Рассмотрите денормализацию данных или использование подзапросов/CTE"
            )
            recommendations.append(diagnose)
            
            # Предлагаем использовать CTE для сложных JOIN
            if join_count > 4:
                optimized_query = convert_joins_to_cte(query)
    
    except Exception as e:
        print(f"Error in join_optimizer rule: {e}")
    
    return recommendations, optimized_query

def convert_joins_to_cte(query: str) -> str:
    """Преобразует сложные JOIN в CTE для улучшения читаемости"""
    # Простая эвристика: находим основной FROM и преобразуем в CTE
    from_match = re.search(r'FROM\s+(\w+)\s+(\w+)', query, re.IGNORECASE)
    if from_match:
        table, alias = from_match.groups()
        cte_query = f"""
WITH {alias}_data AS (
    SELECT * FROM {table}
)
{query.replace(f'FROM {table} {alias}', f'FROM {alias}_data {alias}')}
""".strip()
        return cte_query
    
    return query
