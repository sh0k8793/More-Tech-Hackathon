# backend/src/core/analysis/rules/custom/seq_scan_optimizer.py
from typing import Any, Dict, List, Tuple
import re
from core.models.lint_diagnose import LintDiagnose

def rule_seq_scan_optimizer(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> Tuple[List[LintDiagnose], str]:
    """Обнаруживает Seq Scan и рекомендует индексы, а также оптимизирует запрос"""
    recommendations = []
    optimized_query = query
    
    try:
        if plan and 'Plan' in plan:
            plan_str = str(plan)
            
            # Ищем Seq Scan в плане выполнения
            if 'Seq Scan' in plan_str:
                # Извлекаем имя таблицы
                table_match = re.search(r'Relation Name\": \"(\w+)\"', plan_str)
                table_name = table_match.group(1) if table_match else "unknown"
                
                # Анализируем WHERE условия для рекомендаций по индексам
                where_conditions = extract_where_conditions(query)
                
                # Создаем рекомендацию
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Seq Scan обнаружен на таблице {table_name}",
                    recommendation=f"Добавьте индекс на колонки, используемые в условиях WHERE: {', '.join(where_conditions)}"
                )
                recommendations.append(diagnose)
                
                # Пытаемся оптимизировать запрос, добавляя подсказки для использования индекса
                if where_conditions:
                    optimized_query = add_index_hints(query, where_conditions)
    
    except Exception as e:
        print(f"Error in seq_scan_optimizer rule: {e}")
    
    return recommendations, optimized_query

def extract_where_conditions(query: str) -> List[str]:
    """Извлекает условия WHERE из запроса"""
    conditions = []
    where_match = re.search(r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+GROUP BY|\s+LIMIT|$)', query, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1)
        # Ищем простые условия с колонками
        condition_pattern = r'(\w+)\s*[=<>!]+\s*'
        conditions = re.findall(condition_pattern, where_clause)
    return conditions

def add_index_hints(query: str, conditions: List[str]) -> str:
    """Добавляет комментарии с подсказками по индексам (в реальной системе это были бы настоящие хинты)"""
    # В PostgreSQL нет прямых хинтов как в MySQL, но мы можем добавить комментарии с рекомендациями
    hint_comment = f"/* Consider adding index on: {', '.join(conditions)} */\n"
    return hint_comment + query
