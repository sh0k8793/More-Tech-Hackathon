# backend/src/core/analysis/rules/ast/n_plus_one_optimizer.py
from typing import Any, Dict, List, Tuple
import re
from core.models.lint_diagnose import LintDiagnose

def rule_n_plus_one_optimizer(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> Tuple[List[LintDiagnose], str]:
    """Обнаруживает и оптимизирует N+1 проблемы"""
    recommendations = []
    optimized_query = query
    
    try:
        # Нормализуем запрос
        normalized_query = ' '.join(query.replace('\n', ' ').split())
        
        # 1. Множественные SELECT запросы с похожими условиями
        if normalized_query.count('SELECT') > 1 and normalized_query.count(';') >= 1:
            # Разделяем запросы
            queries = [q.strip() for q in normalized_query.split(';') if q.strip()]
            select_queries = [q for q in queries if q.upper().startswith('SELECT')]
            
            # Ищем запросы с условиями WHERE по ID
            id_queries = []
            for q in select_queries:
                id_match = re.search(r'WHERE\s+(\w+\.)?id\s*=\s*(\d+)', q, re.IGNORECASE)
                if id_match:
                    id_queries.append((q, id_match.group(2)))
            
            # Если нашли несколько запросов по разным ID - это N+1
            if len(id_queries) >= 2:
                # Извлекаем таблицу и условия
                first_query = id_queries[0][0]
                table_match = re.search(r'FROM\s+(\w+)', first_query, re.IGNORECASE)
                
                if table_match:
                    table_name = table_match.group(1)
                    ids = [id_val for _, id_val in id_queries]
                    
                    # Создаем оптимизированный запрос
                    optimized_query = f"SELECT * FROM {table_name} WHERE id IN ({', '.join(ids)})"
                    
                    diagnose = LintDiagnose(
                        line=1,
                        col=1,
                        severity="HIGH",
                        message="Обнаружена N+1 проблема: множественные запросы по разным ID",
                        recommendation=f"Объединено в один запрос с IN условием"
                    )
                    recommendations.append(diagnose)
        
        # 2. IN с подзапросом (может быть неэффективным)
        if re.search(r'IN\s*\(\s*SELECT', normalized_query, re.IGNORECASE):
            # Пытаемся преобразовать IN (SELECT) в JOIN
            in_match = re.search(r'(\w+)\s+IN\s*\(\s*SELECT\s+(\w+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?\s*\)', 
                                normalized_query, re.IGNORECASE)
            
            if in_match:
                left_col, right_col, table, where_condition = in_match.groups()
                join_condition = f"{left_col} = {table}.{right_col}"
                
                # Создаем JOIN версию
                base_query = normalized_query[:in_match.start()]
                optimized_query = f"""
{base_query}
INNER JOIN {table} ON {join_condition}
{"WHERE " + where_condition if where_condition else ""}
""".strip()
                
                diagnose = LintDiagnose(
                    line=1,
                    col=in_match.start() + 1,
                    severity="MEDIUM",
                    message="IN с подзапросом может быть неэффективным",
                    recommendation="Заменено на JOIN для лучшей производительности"
                )
                recommendations.append(diagnose)
    
    except Exception as e:
        print(f"Error in n_plus_one_optimizer rule: {e}")
    
    return recommendations, optimized_query
