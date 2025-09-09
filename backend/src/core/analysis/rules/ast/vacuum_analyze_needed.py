from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_vacuum_analyze_needed(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает таблицы, которым требуется VACUUM или ANALYZE
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие предупреждений о статистике в плане выполнения
        if 'plan' in plan:
            plan_content = str(plan['plan'])

            # Проверяем наличие предупреждений о статистике
            if 'warning' in plan_content.lower() and 'stat' in plan_content.lower():
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message="Обнаружено предупреждение о статистике в плане выполнения",
                    recommendation="Выполните ANALYZE для обновления статистики таблицы. Это поможет планировщику запросов выбрать более эффективный план выполнения."
                )
                recommendations.append(diagnose)

            # Проверяем наличие большого количества "dead tuples" если такая информация доступна
            if 'table_stats' in context:
                for table_name, stats in context['table_stats'].items():
                    if 'dead_tuples' in stats and stats['dead_tuples'] > 10000:
                        diagnose = LintDiagnose(
                            line=1,
                            col=1,
                            severity="MEDIUM",
                            message=f"Таблица {table_name} имеет большое количество мертвых кортежей ({stats['dead_tuples']})",
                            recommendation=f"Выполните VACUUM для таблицы {table_name} для удаления мертвых кортежей и освобождения места. Рассмотрите возможность настройки autovacuum для автоматического выполнения этой операции."
                        )
                        recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in vacuum_analyze_needed rule: {e}")

    return recommendations, optimized_query #ty: ignore
