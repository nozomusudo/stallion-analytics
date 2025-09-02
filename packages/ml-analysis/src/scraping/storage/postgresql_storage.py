"""
PostgreSQLデータベース操作クラス
src/scraping/storage/postgresql_storage.py
"""

import os
import logging
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime
import json

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

from ...database.schemas.race_schema import Race, RaceResult

logger = logging.getLogger(__name__)

class PostgreSQLStorage:
    """PostgreSQL データベース操作クラス"""
    
    def __init__(self):
        """初期化"""
        self.connection_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'stallion_db'),
            'user': os.getenv('POSTGRES_USER', 'stallion_user'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
        
        # 接続テスト
        self._test_connection()
    
    def _test_connection(self):
        """データベース接続テスト"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result:
                        logger.info("PostgreSQL connection test successful")
                    else:
                        raise Exception("Connection test failed")
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """データベース接続のコンテキストマネージャー"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def check_race_exists(self, race_id: str) -> bool:
        """レースが既に存在するかチェック"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM races WHERE race_id = %s LIMIT 1",
                        (race_id,)
                    )
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking race existence for {race_id}: {e}")
            return False
    
    def check_horse_exists(self, horse_id: str) -> bool:
        """馬が既に存在するかチェック"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM horses WHERE id = %s LIMIT 1",
                        (horse_id,)
                    )
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking horse existence for {horse_id}: {e}")
            return False
    
    def insert_horse_basic(self, horse_data: Dict[str, Any]) -> bool:
        """基本馬情報を挿入"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO horses (id, name_ja, created_at, updated_at)
                        VALUES (%(id)s, %(name_ja)s, %(created_at)s, %(updated_at)s)
                        ON CONFLICT (id) DO UPDATE SET
                            updated_at = EXCLUDED.updated_at
                    """, horse_data)
                    conn.commit()
                    logger.debug(f"Horse inserted/updated: {horse_data['id']} ({horse_data['name_ja']})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting horse {horse_data.get('id', 'Unknown')}: {e}")
            return False
    
    def insert_horse_full(self, horse_data: Dict[str, Any]) -> bool:
        """完全な馬情報を挿入・更新"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # birth_dateの変換
                    birth_date = None
                    if horse_data.get('birth_date'):
                        birth_date = datetime.strptime(horse_data['birth_date'], '%Y-%m-%d').date()
                    
                    cursor.execute("""
                        INSERT INTO horses (
                            id, name_ja, name_en, birth_date, sex, 
                            sire_id, dam_id, maternal_grandsire_id, profile,
                            created_at, updated_at
                        )
                        VALUES (
                            %(id)s, %(name_ja)s, %(name_en)s, %(birth_date)s, %(sex)s,
                            %(sire_id)s, %(dam_id)s, %(maternal_grandsire_id)s, %(profile)s,
                            NOW(), NOW()
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            name_en = EXCLUDED.name_en,
                            birth_date = EXCLUDED.birth_date,
                            sex = EXCLUDED.sex,
                            sire_id = EXCLUDED.sire_id,
                            dam_id = EXCLUDED.dam_id,
                            maternal_grandsire_id = EXCLUDED.maternal_grandsire_id,
                            profile = EXCLUDED.profile,
                            updated_at = NOW()
                    """, {
                        'id': horse_data['id'],
                        'name_ja': horse_data['name_ja'],
                        'name_en': horse_data.get('name_en'),
                        'birth_date': birth_date,
                        'sex': horse_data.get('sex'),
                        'sire_id': horse_data.get('sire_id'),
                        'dam_id': horse_data.get('dam_id'),
                        'maternal_grandsire_id': horse_data.get('maternal_grandsire_id'),
                        'profile': json.dumps(horse_data.get('profile')) if horse_data.get('profile') else None
                    })
                    conn.commit()
                    logger.debug(f"Horse full data inserted/updated: {horse_data['id']}")
                    return True
        except Exception as e:
            logger.error(f"Error inserting full horse data {horse_data.get('id', 'Unknown')}: {e}")
            return False
    
    def insert_horse_relations(self, relations: List[Dict]) -> bool:
        """血統関係をPostgreSQLに保存"""
        if not relations:
            return False
        
        try:
            saved_count = 0
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for relation in relations:
                        # 重複チェック
                        cursor.execute("""
                            SELECT id FROM horse_relations 
                            WHERE horse_a_id = %s AND horse_b_id = %s AND relation_type = %s
                        """, (
                            relation['horse_a_id'], 
                            relation['horse_b_id'], 
                            relation['relation_type']
                        ))
                        
                        existing = cursor.fetchone()
                        
                        if not existing:
                            cursor.execute("""
                                INSERT INTO horse_relations (
                                    horse_a_id, horse_b_id, relation_type, children_ids, created_at
                                )
                                VALUES (%s, %s, %s, %s, NOW())
                            """, (
                                relation['horse_a_id'],
                                relation['horse_b_id'], 
                                relation['relation_type'],
                                relation.get('children_ids')
                            ))
                            saved_count += 1
                            logger.debug(f"Relation saved: {relation['relation_type']} ({relation['horse_a_id']} -> {relation['horse_b_id']})")
                        else:
                            logger.debug(f"Relation already exists: {relation['relation_type']}")
                    
                    conn.commit()
                    logger.info(f"Saved {saved_count} relations")
                    return True
            
        except Exception as e:
            logger.error(f"Error saving relations: {e}")
            return False
    
    def insert_race(self, race: Race) -> bool:
        """レース基本情報を挿入"""
        try:
            logger.info(f"Race Is: {race}")
            
            race_data = {
                'race_id': race.race_id,
                'race_date': race.race_date,
                'track_name': race.track_name,
                'race_number': race.race_number,
                'race_name': race.race_name,
                'distance': race.distance,
                'track_type': race.track_type,
                'total_horses': race.total_horses,
                'grade': race.grade,
                'track_direction': race.track_direction,
                'weather': race.weather,
                'track_condition': race.track_condition,
                'start_time': race.start_time,
                'winning_time': race.winning_time,
                'pace': race.pace,
                'prize_1st': race.prize_1st,
                'race_class': race.race_class,
                'race_conditions': race.race_conditions,
                'created_at': race.created_at or datetime.now(),
                'updated_at': race.updated_at or datetime.now()
            }
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO races (
                            race_id, race_date, track_name, race_number, race_name,
                            distance, track_type, total_horses, grade, track_direction,
                            weather, track_condition, start_time, winning_time, pace,
                            prize_1st, race_class, race_conditions, created_at, updated_at
                        ) VALUES (
                            %(race_id)s, %(race_date)s, %(track_name)s, %(race_number)s, %(race_name)s,
                            %(distance)s, %(track_type)s, %(total_horses)s, %(grade)s, %(track_direction)s,
                            %(weather)s, %(track_condition)s, %(start_time)s, %(winning_time)s, %(pace)s,
                            %(prize_1st)s, %(race_class)s, %(race_conditions)s, %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (race_id) DO UPDATE SET
                            race_date = EXCLUDED.race_date,
                            track_name = EXCLUDED.track_name,
                            race_number = EXCLUDED.race_number,
                            race_name = EXCLUDED.race_name,
                            distance = EXCLUDED.distance,
                            track_type = EXCLUDED.track_type,
                            total_horses = EXCLUDED.total_horses,
                            grade = EXCLUDED.grade,
                            track_direction = EXCLUDED.track_direction,
                            weather = EXCLUDED.weather,
                            track_condition = EXCLUDED.track_condition,
                            start_time = EXCLUDED.start_time,
                            winning_time = EXCLUDED.winning_time,
                            pace = EXCLUDED.pace,
                            prize_1st = EXCLUDED.prize_1st,
                            race_class = EXCLUDED.race_class,
                            race_conditions = EXCLUDED.race_conditions,
                            updated_at = EXCLUDED.updated_at
                    """, race_data)
                    conn.commit()
                    logger.debug(f"Race inserted/updated: {race.race_id} ({race.race_name})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting race {race.race_id}: {e}")
            return False
    
    def insert_race_results(self, results: List[RaceResult]) -> bool:
        """レース結果を一括挿入"""
        if not results:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # バッチインサート用のデータ準備
                    insert_data = []
                    for result in results:
                        data = {
                            'race_id': result.race_id,
                            'horse_id': result.horse_id,
                            'horse_name': result.horse_name,
                            'bracket_number': result.bracket_number,
                            'horse_number': result.horse_number,
                            'age': result.age,
                            'sex': result.sex,
                            'jockey_weight': result.jockey_weight,
                            'jockey_name': result.jockey_name,
                            'trainer_name': result.trainer_name,
                            'finish_position': result.finish_position,
                            'jockey_id': result.jockey_id,
                            'trainer_region': result.trainer_region,
                            'trainer_id': result.trainer_id,
                            'race_time': result.race_time,
                            'time_diff': result.time_diff,
                            'passing_order': result.passing_order,
                            'last_3f': result.last_3f,
                            'odds': result.odds,
                            'popularity': result.popularity,
                            'horse_weight': result.horse_weight,
                            'weight_change': result.weight_change,
                            'prize_money': result.prize_money,
                            'owner_id': result.owner_id,
                            'owner_name': result.owner_name,
                            'created_at': result.created_at or datetime.now(),
                            'updated_at': result.updated_at or datetime.now()
                        }

                        # 馬がすでに存在しない場合は追加
                        if not self.check_horse_exists(result.horse_id):
                            horse_data = {
                                'id': result.horse_id,
                                'name_ja': result.horse_name,
                                'created_at': datetime.now(),
                                'updated_at': datetime.now()
                            }
                            self.insert_horse_basic(horse_data)

                        insert_data.append(data)
                    
                    # バッチインサート実行
                    cursor.executemany("""
                        INSERT INTO race_results (
                            race_id, horse_id, horse_name, bracket_number, horse_number,
                            age, sex, jockey_weight, jockey_name, trainer_name,
                            finish_position, jockey_id, trainer_region, trainer_id,
                            race_time, time_diff, passing_order, last_3f, odds,
                            popularity, horse_weight, weight_change, prize_money,
                            owner_id, owner_name, created_at, updated_at
                        ) VALUES (
                            %(race_id)s, %(horse_id)s, %(horse_name)s, %(bracket_number)s, %(horse_number)s,
                            %(age)s, %(sex)s, %(jockey_weight)s, %(jockey_name)s, %(trainer_name)s,
                            %(finish_position)s, %(jockey_id)s, %(trainer_region)s, %(trainer_id)s,
                            %(race_time)s, %(time_diff)s, %(passing_order)s, %(last_3f)s, %(odds)s,
                            %(popularity)s, %(horse_weight)s, %(weight_change)s, %(prize_money)s,
                            %(owner_id)s, %(owner_name)s, %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (race_id, horse_id) DO UPDATE SET
                            updated_at = EXCLUDED.updated_at,
                            finish_position = EXCLUDED.finish_position,
                            race_time = EXCLUDED.race_time,
                            odds = EXCLUDED.odds,
                            popularity = EXCLUDED.popularity,
                            horse_weight = EXCLUDED.horse_weight,
                            weight_change = EXCLUDED.weight_change
                    """, insert_data)
                    
                    conn.commit()
                    logger.debug(f"Race results inserted: {len(results)} records for race {results[0].race_id}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error inserting race results: {e}")
            return False
    
    def insert_complete_race_data(self, race: Race, results: List[RaceResult]) -> bool:
        """レース情報と結果を一括で挿入（トランザクション）"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # トランザクション開始（自動）
                    
                    # 1. レース基本情報挿入
                    if not self._insert_race_with_cursor(cursor, race):
                        conn.rollback()
                        return False
                    
                    # 2. 馬情報確認・追加
                    horses_processed = set()
                    for result in results:
                        if result.horse_id not in horses_processed:
                            if not self.check_horse_exists(result.horse_id):
                                horse_data = {
                                    'id': result.horse_id,
                                    'name_ja': result.horse_name,
                                    'created_at': datetime.now(),
                                    'updated_at': datetime.now()
                                }
                                if not self._insert_horse_with_cursor(cursor, horse_data):
                                    logger.warning(f"Failed to insert horse: {result.horse_id}")
                            horses_processed.add(result.horse_id)
                    
                    # 3. レース結果挿入
                    if not self._insert_race_results_with_cursor(cursor, results):
                        conn.rollback()
                        return False
                    
                    # 全て成功した場合にコミット
                    conn.commit()
                    logger.info(f"Complete race data inserted successfully: {race.race_id}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error inserting complete race data for {race.race_id}: {e}")
            return False
    
    def _insert_race_with_cursor(self, cursor, race: Race) -> bool:
        """カーソルを使ったレース挿入（トランザクション内用）"""
        try:
            distance_int = int(race.distance) if isinstance(race.distance, str) and race.distance.isdigit() else 0
            
            race_data = {
                'race_id': race.race_id,
                'race_date': race.race_date,
                'track_name': race.track_name,
                'race_number': race.race_number,
                'race_name': race.race_name,
                'distance': distance_int,
                'track_type': race.track_type,
                'total_horses': race.total_horses,
                'grade': race.grade,
                'track_direction': race.track_direction,
                'weather': race.weather,
                'track_condition': race.track_condition,
                'start_time': race.start_time,
                'winning_time': race.winning_time,
                'pace': race.pace,
                'prize_1st': race.prize_1st,
                'race_class': race.race_class,
                'race_conditions': race.race_conditions,
                'created_at': race.created_at or datetime.now(),
                'updated_at': race.updated_at or datetime.now()
            }
            
            cursor.execute("""
                INSERT INTO races (
                    race_id, race_date, track_name, race_number, race_name,
                    distance, track_type, total_horses, grade, track_direction,
                    weather, track_condition, start_time, winning_time, pace,
                    prize_1st, race_class, race_conditions, created_at, updated_at
                ) VALUES (
                    %(race_id)s, %(race_date)s, %(track_name)s, %(race_number)s, %(race_name)s,
                    %(distance)s, %(track_type)s, %(total_horses)s, %(grade)s, %(track_direction)s,
                    %(weather)s, %(track_condition)s, %(start_time)s, %(winning_time)s, %(pace)s,
                    %(prize_1st)s, %(race_class)s, %(race_conditions)s, %(created_at)s, %(updated_at)s
                )
                ON CONFLICT (race_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at
            """, race_data)
            return True
        except Exception as e:
            logger.error(f"Error inserting race with cursor: {e}")
            return False
    
    def _insert_horse_with_cursor(self, cursor, horse_data: Dict[str, Any]) -> bool:
        """カーソルを使った馬挿入（トランザクション内用）"""
        try:
            cursor.execute("""
                INSERT INTO horses (id, name_ja, created_at, updated_at)
                VALUES (%(id)s, %(name_ja)s, %(created_at)s, %(updated_at)s)
                ON CONFLICT (id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at
            """, horse_data)
            return True
        except Exception as e:
            logger.error(f"Error inserting horse with cursor: {e}")
            return False
    
    def _insert_race_results_with_cursor(self, cursor, results: List[RaceResult]) -> bool:
        """カーソルを使ったレース結果挿入（トランザクション内用）"""
        try:
            insert_data = []
            for result in results:
                data = {
                    'race_id': result.race_id,
                    'horse_id': result.horse_id,
                    'horse_name': result.horse_name,
                    'bracket_number': result.bracket_number,
                    'horse_number': result.horse_number,
                    'age': result.age,
                    'sex': result.sex,
                    'jockey_weight': result.jockey_weight,
                    'jockey_name': result.jockey_name,
                    'trainer_name': result.trainer_name,
                    'finish_position': result.finish_position,
                    'jockey_id': result.jockey_id,
                    'trainer_region': result.trainer_region,
                    'trainer_id': result.trainer_id,
                    'race_time': result.race_time,
                    'time_diff': result.time_diff,
                    'passing_order': result.passing_order,
                    'last_3f': result.last_3f,
                    'odds': result.odds,
                    'popularity': result.popularity,
                    'horse_weight': result.horse_weight,
                    'weight_change': result.weight_change,
                    'prize_money': result.prize_money,
                    'owner_id': result.owner_id,
                    'owner_name': result.owner_name,
                    'created_at': result.created_at or datetime.now(),
                    'updated_at': result.updated_at or datetime.now()
                }
                insert_data.append(data)
            
            cursor.executemany("""
                INSERT INTO race_results (
                    race_id, horse_id, horse_name, bracket_number, horse_number,
                    age, sex, jockey_weight, jockey_name, trainer_name,
                    finish_position, jockey_id, trainer_region, trainer_id,
                    race_time, time_diff, passing_order, last_3f, odds,
                    popularity, horse_weight, weight_change, prize_money,
                    owner_id, owner_name, created_at, updated_at
                ) VALUES (
                    %(race_id)s, %(horse_id)s, %(horse_name)s, %(bracket_number)s, %(horse_number)s,
                    %(age)s, %(sex)s, %(jockey_weight)s, %(jockey_name)s, %(trainer_name)s,
                    %(finish_position)s, %(jockey_id)s, %(trainer_region)s, %(trainer_id)s,
                    %(race_time)s, %(time_diff)s, %(passing_order)s, %(last_3f)s, %(odds)s,
                    %(popularity)s, %(horse_weight)s, %(weight_change)s, %(prize_money)s,
                    %(owner_id)s, %(owner_name)s, %(created_at)s, %(updated_at)s
                )
                ON CONFLICT (race_id, horse_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    finish_position = EXCLUDED.finish_position,
                    race_time = EXCLUDED.race_time,
                    odds = EXCLUDED.odds,
                    popularity = EXCLUDED.popularity,
                    horse_weight = EXCLUDED.horse_weight,
                    weight_change = EXCLUDED.weight_change
            """, insert_data)
            return True
        except Exception as e:
            logger.error(f"Error inserting race results with cursor: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """データベース統計情報を取得"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    stats = {}
                    
                    # 各テーブルの件数
                    tables = ['races', 'race_results', 'horses', 'horse_relations']
                    for table in tables:
                        cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                        result = cursor.fetchone()
                        stats[f'{table}_count'] = result['count'] if result else 0
                    
                    # 最新レース日
                    cursor.execute("SELECT MAX(race_date) as latest_race_date FROM races")
                    result = cursor.fetchone()
                    stats['latest_race_date'] = result['latest_race_date'] if result['latest_race_date'] else None
                    
                    # G1レース数
                    cursor.execute("SELECT COUNT(*) as g1_count FROM races WHERE grade = 'G1'")
                    result = cursor.fetchone()
                    stats['g1_races_count'] = result['g1_count'] if result else 0
                    
                    return stats
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def delete_race_data(self, race_id: str) -> bool:
        """レースデータを削除（テスト用）"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # レース結果を先に削除（外部キー制約）
                    cursor.execute("DELETE FROM race_results WHERE race_id = %s", (race_id,))
                    # レース基本情報を削除
                    cursor.execute("DELETE FROM races WHERE race_id = %s", (race_id,))
                    
                    conn.commit()
                    logger.info(f"Race data deleted: {race_id}")
                    return True
        except Exception as e:
            logger.error(f"Error deleting race data {race_id}: {e}")
            return False