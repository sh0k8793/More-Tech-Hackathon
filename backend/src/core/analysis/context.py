# backend/src/core/analysis/context.py
from typing import Dict, Any
from psycopg import AsyncConnection


# TODO: добавить получение базовой информации о бд

async def get_database_context(connection: AsyncConnection) -> Dict[str, Any]:
    """Получает полный контекст БД для использования в правилах"""
    context = {
        "settings": await get_db_settings(connection),
        "table_stats": await get_table_statistics(connection),
        "index_stats": await get_index_statistics(connection),
        "activity": await get_current_activity(connection),
        "io_stats": await get_io_statistics(connection)
    }
    return context

async def get_db_settings(connection: AsyncConnection):
    """Настройки PostgreSQL доступные для чтения"""
    async with connection.cursor() as cur:
        await cur.execute("""
            SELECT name, setting, unit, context 
            FROM pg_settings 
            WHERE context IN ('user', 'superuser')
            ORDER BY name
        """)
        settings = {}
        async for row in cur:
            settings[row[0]] = {
                'setting': row[1],
                'unit': row[2],
                'context': row[3]
            }
        return settings

async def get_table_statistics(connection: AsyncConnection):
    """Статистика по таблицам"""
    async with connection.cursor() as cur:
        await cur.execute("""
            SELECT 
                schemaname,
                relname,
                n_live_tup,
                n_dead_tup,
                n_mod_since_analyze,
                last_analyze,
                last_autoanalyze,
                pg_size_pretty(pg_total_relation_size(relid)) as total_size,
                pg_size_pretty(pg_relation_size(relid)) as table_size
            FROM pg_stat_user_tables
            ORDER BY schemaname, relname
        """)
        
        tables = {}
        async for row in cur:
            table_key = f"{row[0]}.{row[1]}" if row[0] != 'public' else row[1]
            tables[table_key] = {
                'row_count': row[2],
                'dead_rows': row[3],
                'mods_since_analyze': row[4],
                'last_analyze': row[5],
                'last_autoanalyze': row[6],
                'total_size': row[7],
                'table_size': row[8]
            }
        return tables

async def get_index_statistics(connection: AsyncConnection):
    """Статистика использования индексов"""
    async with connection.cursor() as cur:
        await cur.execute("""
            SELECT 
                schemaname,
                relname as table_name,
                indexrelname as index_name,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size
            FROM pg_stat_user_indexes
            ORDER BY schemaname, relname, indexrelname
        """)
        
        indexes = []
        async for row in cur:
            indexes.append({
                'schema': row[0],
                'table': row[1],
                'index': row[2],
                'scans': row[3],
                'tuples_read': row[4],
                'tuples_fetched': row[5],
                'size': row[6]
            })
        return indexes

async def get_io_statistics(connection: AsyncConnection):
    """Статистика ввода/вывода"""
    async with connection.cursor() as cur:
        await cur.execute("""
            SELECT 
                schemaname,
                relname,
                heap_blks_read,
                heap_blks_hit,
                idx_blks_read,
                idx_blks_hit
            FROM pg_statio_user_tables
            ORDER BY schemaname, relname
        """)
        
        io_stats = {}
        async for row in cur:
            table_key = f"{row[0]}.{row[1]}" if row[0] != 'public' else row[1]
            io_stats[table_key] = {
                'heap_blocks_read': row[2],
                'heap_blocks_hit': row[3],
                'index_blocks_read': row[4],
                'index_blocks_hit': row[5]
            }
        return io_stats

async def get_current_activity(connection: AsyncConnection):
    """Текущая активность БД"""
    async with connection.cursor() as cur:
        await cur.execute("""
            SELECT 
                pid,
                usename,
                application_name,
                client_addr::text,
                query_start,
                state,
                LEFT(query, 100) as query_preview
            FROM pg_stat_activity 
            WHERE datname = current_database() AND state = 'active'
        """)
        
        activity = []
        async for row in cur:
            activity.append({
                'pid': row[0],
                'user': row[1],
                'app_name': row[2],
                'client_addr': row[3],
                'query_start': row[4],
                'state': row[5],
                'query_preview': row[6]
            })
        return activity
