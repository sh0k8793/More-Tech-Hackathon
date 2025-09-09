from typing import Dict, List, Any
from ....models.lint_diagnose import LintDiagnose


def rule_defragmentation_analysis(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости дефрагментации таблиц и индексов
    """
    recommendations = []

    try:
        # Анализ статистики таблиц
        table_stats = context.get('table_stats', {})

        # Анализ статистики индексов
        index_stats = context.get('index_stats', {})

        # Анализ статистики ввода-вывода
        io_stats = context.get('io_stats', {})

        # Рекомендации по дефрагментации таблиц
        table_defrag_recommendations = analyze_table_defragmentation(table_stats, io_stats)
        recommendations.extend(table_defrag_recommendations)

        # Рекомендации по дефрагментации индексов
        index_defrag_recommendations = analyze_index_defragmentation(index_stats, table_stats)
        recommendations.extend(index_defrag_recommendations)

        # Рекомендации по анализу таблиц
        analyze_recommendations = analyze_table_analyze(table_stats, context)
        recommendations.extend(analyze_recommendations)

        # Рекомендации по вакуумированию таблиц
        vacuum_recommendations = analyze_table_vacuum(table_stats, context)
        recommendations.extend(vacuum_recommendations)

        # Рекомендации по реиндексации
        reindex_recommendations = analyze_reindexing(index_stats, table_stats)
        recommendations.extend(reindex_recommendations)

        # Рекомендации по настройке autovacuum
        autovacuum_recommendations = analyze_autovacuum_settings_for_defrag(context)
        recommendations.extend(autovacuum_recommendations)

    except Exception as e:
        print(f"Error in defragmentation_analysis rule: {e}")

    return recommendations


def analyze_table_defragmentation(table_stats: Dict[str, Any], io_stats: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости дефрагментации таблиц
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)
            dead_rows = stats.get('dead_rows', 0)
            mods_since_analyze = stats.get('mods_since_analyze', 0)
            last_analyze = stats.get('last_analyze', None)
            stats.get('last_autoanalyze', None)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица имеет много мертвых строк
            if row_count > 0 and dead_rows / row_count > 0.2:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Таблица {table_name} имеет высокую фрагментацию ({dead_rows / row_count:.1%} мертвых строк)",
                    recommendation=f"Рассуйте выполнение VACUUM (FULL) для таблицы {table_name} для дефрагментации"
                ))

            # Если таблица имеет много модификаций с момента последующего анализа
            elif mods_since_analyze > row_count * 0.1:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} имеет много модификаций с момента последующего анализа ({mods_since_analyze})",
                    recommendation=f"Рассуйте выполнение ANALYZE для таблицы {table_name} для обновления статистики"
                ))

            # Если таблица большая и давно не анализировалась
            elif size_in_mb > 100 and last_analyze and is_old(last_analyze, days=30):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} большая ({table_size}) и давно не анализировалась ({last_analyze})",
                    recommendation=f"Рассуйте выполнение ANALYZE для таблицы {table_name} для обновления статистики"
                ))

            # Если таблица очень большая и имеет много мертвых строк
            elif size_in_mb > 1000 and dead_rows > 0:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Таблица {table_name} очень большая ({table_size}) и имеет мертвые строки ({dead_rows})",
                    recommendation=f"Рассуйте выполнение VACUUM (FULL) для таблицы {table_name} для значительной дефрагментации"
                ))

            # Анализ статистики ввода-вывода
            if table_name in io_stats:
                io_stat = io_stats[table_name]
                heap_blocks_read = io_stat.get('heap_blocks_read', 0)
                heap_blocks_hit = io_stat.get('heap_blocks_hit', 0)

                # Если отношение чтения к попаданию высокое
                if heap_blocks_hit > 0 and heap_blocks_read / heap_blocks_hit > 0.5:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="MEDIUM",
                        message=f"Таблица {table_name} имеет высокое отношение чтения к попаданию в кэш ({heap_blocks_read / heap_blocks_hit:.1f})",
                        recommendation=f"Рассуйте дефрагментацию таблицы {table_name} для улучшения кэширования"
                    ))

    except Exception as e:
        print(f"Error analyzing table defragmentation: {e}")

    return recommendations


def analyze_index_defragmentation(index_stats: List[Dict[str, Any]], table_stats: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости дефрагментации индексов
    """
    recommendations = []

    try:
        for idx in index_stats:
            idx.get('table', '')
            index_name = idx.get('index', '')
            index_size = idx.get('size', '0')
            scans = idx.get('scans', 0)
            tuples_read = idx.get('tuples_read', 0)
            tuples_fetched = idx.get('tuples_fetched', 0)

            size_in_mb = parse_memory_value(index_size)

            # Если индекс большой и редко используется
            if size_in_mb > 100 and scans == 0:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Индекс {index_name} большой ({index_size}) и не используется",
                    recommendation=f"Рассуйте удаление неиспользуемого индекса {index_name} для улучшения производительности"
                ))

            # Если индекс большой и часто используется
            elif size_in_mb > 100 and scans > 0:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Индекс {index_name} большой ({index_size}) и часто используется ({scans} сканирований)",
                    recommendation=f"Рассуйте выполнение REINDEX INDEX для индекса {index_name} для дефрагментации"
                ))

            # Если отношение извлеченных кортежей к прочитанным высокое
            if tuples_read > 0 and tuples_fetched / tuples_read > 0.8:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Индекс {index_name} имеет высокое отношение извлеченных кортежей к прочитанным ({tuples_fetched / tuples_read:.1f})",
                    recommendation=f"Рассуйте дефрагментацию индекса {index_name} для улучшения производительности"
                ))

            # Если индекс очень большой
            if size_in_mb > 500:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Индекс {index_name} очень большой ({index_size}), можно рассовать дефрагментацию",
                    recommendation=f"Рассуйте выполнение REINDEX INDEX для индекса {index_name} для улучшения производительности"
                ))

    except Exception as e:
        print(f"Error analyzing index defragmentation: {e}")

    return recommendations


def analyze_table_analyze(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости выполнения ANALYZE
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)
            stats.get('dead_rows', 0)
            mods_since_analyze = stats.get('mods_since_analyze', 0)
            last_analyze = stats.get('last_analyze', None)
            stats.get('last_autoanalyze', None)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица имеет много модификаций с момента последующего анализа
            if mods_since_analyze > row_count * 0.05:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} имеет много модификаций с момента последующего анализа ({mods_since_analyze})",
                    recommendation=f"Рассуйте выполнение ANALYZE для таблицы {table_name} для обновления статистики"
                ))

            # Если таблица большая и давно не анализировалась
            elif size_in_mb > 50 and last_analyze and is_old(last_analyze, days=7):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} большая ({table_size}) и давно не анализировалась ({last_analyze})",
                    recommendation=f"Рассуйте выполнение ANALYZE для таблицы {table_name} для обновления статистики"
                ))

            # Если таблица очень большая и давно не анализировалась
            elif size_in_mb > 500 and last_analyze and is_old(last_analyze, days=30):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} очень большая ({table_size}) и давно не анализировалась ({last_analyze})",
                    recommendation=f"Рассуйте выполнение ANALYZE для таблицы {table_name} для обновления статистики"
                ))

    except Exception as e:
        print(f"Error analyzing table analyze: {e}")

    return recommendations


def analyze_table_vacuum(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости выполнения VACUUM
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)
            dead_rows = stats.get('dead_rows', 0)
            stats.get('mods_since_analyze', 0)
            stats.get('last_analyze', None)
            last_autoanalyze = stats.get('last_autoanalyze', None)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица имеет много мертвых строк
            if row_count > 0 and dead_rows / row_count > 0.1:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} имеет много мертвых строк ({dead_rows / row_count:.1%})",
                    recommendation=f"Рассуйте выполнение VACUUM для таблицы {table_name} для очистки мертвых строк"
                ))

            # Если таблица очень большая и имеет много мертвых строк
            elif size_in_mb > 1000 and dead_rows > 0:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Таблица {table_name} очень большая ({table_size}) и имеет мертвые строки ({dead_rows})",
                    recommendation=f"Рассуйте выполнение VACUUM (FULL) для таблицы {table_name} для значительной дефрагментации"
                ))

            # Если таблица большая и давно не вакуумировалась
            elif size_in_mb > 100 and last_autoanalyze and is_old(last_autoanalyze, days=30):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} большая ({table_size}) и давно не вакуумировалась ({last_autoanalyze})",
                    recommendation=f"Рассуйте выполнение VACUUM для таблицы {table_name} для очистки мертвых строк"
                ))

    except Exception as e:
        print(f"Error analyzing table vacuum: {e}")

    return recommendations


def analyze_reindexing(index_stats: List[Dict[str, Any]], table_stats: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ необходимости реиндексации
    """
    recommendations = []

    try:
        for idx in index_stats:
            idx.get('table', '')
            index_name = idx.get('index', '')
            index_size = idx.get('size', '0')
            scans = idx.get('scans', 0)
            tuples_read = idx.get('tuples_read', 0)
            tuples_fetched = idx.get('tuples_fetched', 0)

            size_in_mb = parse_memory_value(index_size)

            # Если индекс большой и часто используется
            if size_in_mb > 100 and scans > 100:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Индекс {index_name} большой ({index_size}) и часто используется ({scans} сканирований)",
                    recommendation=f"Рассуйте выполнение REINDEX INDEX для индекса {index_name} для дефрагментации"
                ))

            # Если индекс очень большой
            if size_in_mb > 500:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Индекс {index_name} очень большой ({index_size}), можно рассовать реиндексацию",
                    recommendation=f"Рассуйте выполнение REINDEX INDEX для индекса {index_name} для улучшения производительности"
                ))

            # Если отношение извлеченных кортежей к прочитанным очень высокое
            if tuples_read > 0 and tuples_fetched / tuples_read > 0.9:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Индекс {index_name} имеет очень высокое отношение извлеченных кортежей к прочитанным ({tuples_fetched / tuples_read:.1f})",
                    recommendation=f"Рассуйте выполнение REINDEX INDEX для индекса {index_name} для улучшения производительности"
                ))

    except Exception as e:
        print(f"Error analyzing reindexing: {e}")

    return recommendations


def analyze_autovacuum_settings_for_defrag(context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum для дефрагментации
    """
    recommendations = []

    try:
        db_settings = context.get('settings', {})

        # Анализ autovacuum_vacuum_scale_factor
        autovacuum_vacuum_scale_factor = float(db_settings.get('autovacuum_vacuum_scale_factor', {}).get('setting', '0.2'))

        # Если autovacuum_vacuum_scale_factor слишком большой
        if autovacuum_vacuum_scale_factor > 0.3:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_scale_factor установлен в {autovacuum_vacuum_scale_factor}, что может вызывать запаздывание очистки",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_scale_factor до 0.1-0.2 для более частой очистки"
            ))

        # Анализ autovacuum_analyze_scale_factor
        autovacuum_analyze_scale_factor = float(db_settings.get('autovacuum_analyze_scale_factor', {}).get('setting', '0.1'))

        # Если autovacuum_analyze_scale_factor слишком большой
        if autovacuum_analyze_scale_factor > 0.2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_scale_factor установлен в {autovacuum_analyze_scale_factor}, что может вызывать запаздывание анализа",
                recommendation="Рассуйте уменьшение autovacuum_analyze_scale_factor до 0.05-0.1 для более частого анализа"
            ))

        # Анализ autovacuum_vacuum_threshold
        autovacuum_vacuum_threshold = int(db_settings.get('autovacuum_vacuum_threshold', {}).get('setting', '50'))

        # Если autovacuum_vacuum_threshold слишком большой
        if autovacuum_vacuum_threshold > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_threshold установлен в {autovacuum_vacuum_threshold}, что может вызывать запаздывание очистки",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_threshold до 50 для более частой очистки"
            ))

        # Анализ autovacuum_analyze_threshold
        autovacuum_analyze_threshold = int(db_settings.get('autovacuum_analyze_threshold', {}).get('setting', '50'))

        # Если autovacuum_analyze_threshold слишком большой
        if autovacuum_analyze_threshold > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_threshold установлен в {autovacuum_analyze_threshold}, что может вызывать запаздывание анализа",
                recommendation="Рассуйте уменьшение autovacuum_analyze_threshold до 50 для более частого анализа"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum settings for defrag: {e}")

    return recommendations


def is_old(timestamp_str: str, days: int = 30) -> bool:
    """
    Проверяет, является ли метка времени старше указанного количества дней
    """
    try:
        from datetime import datetime, timedelta

        # Преобразование строки в datetime
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

        # Вычисление разницы во времени
        delta = datetime.now() - timestamp

        # Проверка, больше ли разница указанного количества дней
        return delta > timedelta(days=days)

    except Exception:
        return True  # Если не удалось распарсить, считаем старым


def parse_memory_value(value_str: str) -> int:
    """
    Парсинг строкового значения памяти в мегабайты
    """
    try:
        value_str = value_str.strip().upper()

        if value_str.endswith('KB'):
            return int(float(value_str[:-2]) / 1024)
        elif value_str.endswith('MB'):
            return int(float(value_str[:-2]))
        elif value_str.endswith('GB'):
            return int(float(value_str[:-2]) * 1024)
        elif value_str.endswith('TB'):
            return int(float(value_str[:-2]) * 1024 * 1024)
        else:
            # Предполагаем, что это уже в байтах
            return int(int(float(value_str)) / (1024 * 1024))
    except Exception:
        return 0
