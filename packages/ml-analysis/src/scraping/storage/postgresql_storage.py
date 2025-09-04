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

from ...database.schemas.race_schema import Race, RaceResult, RacePayout
from ...database.schemas.jockey_schema import Jockey
from ...database.schemas.trainer_schema import Trainer
from ...database.schemas.owner_schema import Owner
from ...database.schemas.breeder_schema import Breeder

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
            
            corner_positions_json = None
            lap_data_json = None
            
            if race.corner_positions:
                corner_positions_json = json.dumps(race.corner_positions, ensure_ascii=False)
            
            if race.lap_data:
                lap_data_json = json.dumps(race.lap_data, ensure_ascii=False)
            
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
                'corner_positions': corner_positions_json,
                'lap_data': lap_data_json,
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
                            prize_1st, race_class, race_conditions, corner_positions, lap_data,
                            created_at, updated_at
                        ) VALUES (
                            %(race_id)s, %(race_date)s, %(track_name)s, %(race_number)s, %(race_name)s,
                            %(distance)s, %(track_type)s, %(total_horses)s, %(grade)s, %(track_direction)s,
                            %(weather)s, %(track_condition)s, %(start_time)s, %(winning_time)s, %(pace)s,
                            %(prize_1st)s, %(race_class)s, %(race_conditions)s, %(corner_positions)s::jsonb, %(lap_data)s::jsonb,
                            %(created_at)s, %(updated_at)s
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
                            corner_positions = EXCLUDED.corner_positions,
                            lap_data = EXCLUDED.lap_data,
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
    
    def insert_race_payouts(self, payouts: List[RacePayout]) -> bool:
        """払い戻し情報を一括挿入"""
        if not payouts:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # バッチインサート用のデータ準備
                    insert_data = []
                    for payout in payouts:
                        data = {
                            'race_id': payout.race_id,
                            'bet_type': payout.bet_type,
                            'combination': payout.combination,
                            'payout_amount': payout.payout_amount,
                            'popularity': payout.popularity,
                            'created_at': payout.created_at or datetime.now(),
                            'updated_at': payout.updated_at or datetime.now()
                        }
                        insert_data.append(data)
                    
                    # バッチインサート実行
                    cursor.executemany("""
                        INSERT INTO race_payouts (
                            race_id, bet_type, combination, payout_amount, popularity,
                            created_at, updated_at
                        ) VALUES (
                            %(race_id)s, %(bet_type)s, %(combination)s, %(payout_amount)s, %(popularity)s,
                            %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (race_id, bet_type, combination) DO UPDATE SET
                            payout_amount = EXCLUDED.payout_amount,
                            popularity = EXCLUDED.popularity,
                            updated_at = EXCLUDED.updated_at
                    """, insert_data)
                    
                    conn.commit()
                    logger.debug(f"Race payouts inserted: {len(payouts)} records for race {payouts[0].race_id if payouts else 'unknown'}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error inserting race payouts: {e}")
            return False
    
    def insert_jockey(self, jockey: Jockey) -> bool:
        """騎手情報を挿入"""
        try:
            logger.info(f"Jockey Is: {jockey}")
            
            yearly_stats_json = None
            track_stats_json = None
            distance_stats_json = None
            
            if jockey.yearly_stats:
                yearly_stats_json = json.dumps(jockey.yearly_stats, ensure_ascii=False)
            
            if jockey.track_stats:
                track_stats_json = json.dumps(jockey.track_stats, ensure_ascii=False)
                
            if jockey.distance_stats:
                distance_stats_json = json.dumps(jockey.distance_stats, ensure_ascii=False)
            
            jockey_data = {
                'jockey_id': jockey.jockey_id,
                'name_ja': jockey.name_ja,
                'name_en': jockey.name_en,
                'birthdate': jockey.birthdate,
                'region': jockey.region,
                'license_type': jockey.license_type,
                'trainer_name': jockey.trainer_name,
                'debut_date': jockey.debut_date,
                'status': jockey.status,
                'weight': jockey.weight,
                'height': jockey.height,
                'total_races': jockey.total_races,
                'wins': jockey.wins,
                'seconds': jockey.seconds,
                'thirds': jockey.thirds,
                'win_rate': jockey.win_rate,
                'show_rate': jockey.show_rate,
                'total_prize_money': jockey.total_prize_money,
                'yearly_stats': yearly_stats_json,
                'track_stats': track_stats_json,
                'distance_stats': distance_stats_json,
                'created_at': jockey.created_at or datetime.now(),
                'updated_at': jockey.updated_at or datetime.now()
            }
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO jockeys (
                            jockey_id, name_ja, name_en, birthdate, region, license_type, trainer_name,
                            debut_date, status, weight, height, total_races, wins,
                            seconds, thirds, win_rate, show_rate, total_prize_money,
                            yearly_stats, track_stats, distance_stats, created_at, updated_at
                        ) VALUES (
                            %(jockey_id)s, %(name_ja)s, %(name_en)s, %(birthdate)s, %(region)s, %(license_type)s, %(trainer_name)s,
                            %(debut_date)s, %(status)s, %(weight)s, %(height)s, %(total_races)s, %(wins)s,
                            %(seconds)s, %(thirds)s, %(win_rate)s, %(show_rate)s, %(total_prize_money)s,
                            %(yearly_stats)s::jsonb, %(track_stats)s::jsonb, %(distance_stats)s::jsonb,
                            %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (jockey_id) DO UPDATE SET
                            name_ja = EXCLUDED.name_ja,
                            name_en = EXCLUDED.name_en,
                            birthdate = EXCLUDED.birthdate,
                            region = EXCLUDED.region,
                            license_type = EXCLUDED.license_type,
                            trainer_name = EXCLUDED.trainer_name,
                            debut_date = EXCLUDED.debut_date,
                            status = EXCLUDED.status,
                            weight = EXCLUDED.weight,
                            height = EXCLUDED.height,
                            total_races = EXCLUDED.total_races,
                            wins = EXCLUDED.wins,
                            seconds = EXCLUDED.seconds,
                            thirds = EXCLUDED.thirds,
                            win_rate = EXCLUDED.win_rate,
                            show_rate = EXCLUDED.show_rate,
                            total_prize_money = EXCLUDED.total_prize_money,
                            yearly_stats = EXCLUDED.yearly_stats,
                            track_stats = EXCLUDED.track_stats,
                            distance_stats = EXCLUDED.distance_stats,
                            updated_at = EXCLUDED.updated_at
                    """, jockey_data)
                    conn.commit()
                    logger.debug(f"Jockey inserted/updated: {jockey.jockey_id} ({jockey.name_ja})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting jockey {jockey.jockey_id}: {e}")
            return False

    def insert_trainer(self, trainer: Trainer) -> bool:
        """調教師情報を挿入"""
        try:
            logger.info(f"Trainer Is: {trainer}")
            
            yearly_stats_json = None
            race_stats_json = None
            track_stats_json = None
            distance_stats_json = None
            
            if trainer.yearly_stats:
                yearly_stats_json = json.dumps(trainer.yearly_stats, ensure_ascii=False)
            
            if trainer.race_stats:
                race_stats_json = json.dumps(trainer.race_stats, ensure_ascii=False)
            
            if trainer.track_stats:
                track_stats_json = json.dumps(trainer.track_stats, ensure_ascii=False)
                
            if trainer.distance_stats:
                distance_stats_json = json.dumps(trainer.distance_stats, ensure_ascii=False)
            
            trainer_data = {
                'trainer_id': trainer.trainer_id,
                'name_ja': trainer.name_ja,
                'name_en': trainer.name_en,
                'birthdate': trainer.birthdate,
                'region': trainer.region,
                'license_type': trainer.license_type,
                'debut_date': trainer.debut_date,
                'status': trainer.status,
                'total_races': trainer.total_races,
                'wins': trainer.wins,
                'seconds': trainer.seconds,
                'thirds': trainer.thirds,
                'win_rate': trainer.win_rate,
                'second_rate': trainer.second_rate,
                'show_rate': trainer.show_rate,
                'total_prize_money': trainer.total_prize_money,
                'yearly_stats': yearly_stats_json,
                'race_stats': race_stats_json,
                'track_stats': track_stats_json,
                'distance_stats': distance_stats_json,
                'created_at': trainer.created_at or datetime.now(),
                'updated_at': trainer.updated_at or datetime.now()
            }
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trainers (
                            trainer_id, name_ja, name_en, birthdate, region, license_type,
                            debut_date, status, total_races, wins, seconds, thirds,
                            win_rate, second_rate, show_rate, total_prize_money,
                            yearly_stats, race_stats, track_stats, distance_stats,
                            created_at, updated_at
                        ) VALUES (
                            %(trainer_id)s, %(name_ja)s, %(name_en)s, %(birthdate)s, %(region)s, %(license_type)s,
                            %(debut_date)s, %(status)s, %(total_races)s, %(wins)s, %(seconds)s, %(thirds)s,
                            %(win_rate)s, %(second_rate)s, %(show_rate)s, %(total_prize_money)s,
                            %(yearly_stats)s::jsonb, %(race_stats)s::jsonb, %(track_stats)s::jsonb, %(distance_stats)s::jsonb,
                            %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (trainer_id) DO UPDATE SET
                            name_ja = EXCLUDED.name_ja,
                            name_en = EXCLUDED.name_en,
                            birthdate = EXCLUDED.birthdate,
                            region = EXCLUDED.region,
                            license_type = EXCLUDED.license_type,
                            debut_date = EXCLUDED.debut_date,
                            status = EXCLUDED.status,
                            total_races = EXCLUDED.total_races,
                            wins = EXCLUDED.wins,
                            seconds = EXCLUDED.seconds,
                            thirds = EXCLUDED.thirds,
                            win_rate = EXCLUDED.win_rate,
                            second_rate = EXCLUDED.second_rate,
                            show_rate = EXCLUDED.show_rate,
                            total_prize_money = EXCLUDED.total_prize_money,
                            yearly_stats = EXCLUDED.yearly_stats,
                            race_stats = EXCLUDED.race_stats,
                            track_stats = EXCLUDED.track_stats,
                            distance_stats = EXCLUDED.distance_stats,
                            updated_at = EXCLUDED.updated_at
                    """, trainer_data)
                    conn.commit()
                    logger.debug(f"Trainer inserted/updated: {trainer.trainer_id} ({trainer.name_ja})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting trainer {trainer.trainer_id}: {e}")
            return False

    def insert_owner(self, owner: Owner) -> bool:
        """馬主情報を挿入"""
        try:
            logger.info(f"Owner Is: {owner}")
            
            yearly_stats_json = None
            race_stats_json = None
            track_stats_json = None
            distance_stats_json = None
            horse_list_json = None
            
            if owner.yearly_stats:
                yearly_stats_json = json.dumps(owner.yearly_stats, ensure_ascii=False)
            
            if owner.race_stats:
                race_stats_json = json.dumps(owner.race_stats, ensure_ascii=False)
            
            if owner.track_stats:
                track_stats_json = json.dumps(owner.track_stats, ensure_ascii=False)
                
            if owner.distance_stats:
                distance_stats_json = json.dumps(owner.distance_stats, ensure_ascii=False)
                
            if owner.horse_list:
                horse_list_json = json.dumps(owner.horse_list, ensure_ascii=False)
            
            owner_data = {
                'owner_id': owner.owner_id,
                'name_ja': owner.name_ja,
                'name_en': owner.name_en,
                'birthdate': owner.birthdate,
                'owner_type': owner.owner_type,
                'license_date': owner.license_date,
                'status': owner.status,
                'total_races': owner.total_races,
                'wins': owner.wins,
                'seconds': owner.seconds,
                'thirds': owner.thirds,
                'win_rate': owner.win_rate,
                'second_rate': owner.second_rate,
                'show_rate': owner.show_rate,
                'total_prize_money': owner.total_prize_money,
                'total_horses': owner.total_horses,
                'active_horses': owner.active_horses,
                'retired_horses': owner.retired_horses,
                'stakes_wins': owner.stakes_wins,
                'grade1_wins': owner.grade1_wins,
                'yearly_stats': yearly_stats_json,
                'race_stats': race_stats_json,
                'track_stats': track_stats_json,
                'distance_stats': distance_stats_json,
                'horse_list': horse_list_json,
                'created_at': owner.created_at or datetime.now(),
                'updated_at': owner.updated_at or datetime.now()
            }
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO owners (
                            owner_id, name_ja, name_en, birthdate, owner_type, license_date,
                            status, total_races, wins, seconds, thirds,
                            win_rate, second_rate, show_rate, total_prize_money,
                            total_horses, active_horses, retired_horses, stakes_wins, grade1_wins,
                            yearly_stats, race_stats, track_stats, distance_stats, horse_list,
                            created_at, updated_at
                        ) VALUES (
                            %(owner_id)s, %(name_ja)s, %(name_en)s, %(birthdate)s, %(owner_type)s, %(license_date)s,
                            %(status)s, %(total_races)s, %(wins)s, %(seconds)s, %(thirds)s,
                            %(win_rate)s, %(second_rate)s, %(show_rate)s, %(total_prize_money)s,
                            %(total_horses)s, %(active_horses)s, %(retired_horses)s, %(stakes_wins)s, %(grade1_wins)s,
                            %(yearly_stats)s::jsonb, %(race_stats)s::jsonb, %(track_stats)s::jsonb, %(distance_stats)s::jsonb, %(horse_list)s::jsonb,
                            %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (owner_id) DO UPDATE SET
                            name_ja = EXCLUDED.name_ja,
                            name_en = EXCLUDED.name_en,
                            birthdate = EXCLUDED.birthdate,
                            owner_type = EXCLUDED.owner_type,
                            license_date = EXCLUDED.license_date,
                            status = EXCLUDED.status,
                            total_races = EXCLUDED.total_races,
                            wins = EXCLUDED.wins,
                            seconds = EXCLUDED.seconds,
                            thirds = EXCLUDED.thirds,
                            win_rate = EXCLUDED.win_rate,
                            second_rate = EXCLUDED.second_rate,
                            show_rate = EXCLUDED.show_rate,
                            total_prize_money = EXCLUDED.total_prize_money,
                            total_horses = EXCLUDED.total_horses,
                            active_horses = EXCLUDED.active_horses,
                            retired_horses = EXCLUDED.retired_horses,
                            stakes_wins = EXCLUDED.stakes_wins,
                            grade1_wins = EXCLUDED.grade1_wins,
                            yearly_stats = EXCLUDED.yearly_stats,
                            race_stats = EXCLUDED.race_stats,
                            track_stats = EXCLUDED.track_stats,
                            distance_stats = EXCLUDED.distance_stats,
                            horse_list = EXCLUDED.horse_list,
                            updated_at = EXCLUDED.updated_at
                    """, owner_data)
                    conn.commit()
                    logger.debug(f"Owner inserted/updated: {owner.owner_id} ({owner.name_ja})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting owner {owner.owner_id}: {e}")
            return False

    def insert_breeder(self, breeder: Breeder) -> bool:
        """生産者情報を挿入"""
        try:
            logger.info(f"Breeder Is: {breeder}")
            
            yearly_stats_json = None
            race_stats_json = None
            track_stats_json = None
            distance_stats_json = None
            produced_horses_json = None
            stallion_stats_json = None
            
            if breeder.yearly_stats:
                yearly_stats_json = json.dumps(breeder.yearly_stats, ensure_ascii=False)
            
            if breeder.race_stats:
                race_stats_json = json.dumps(breeder.race_stats, ensure_ascii=False)
            
            if breeder.track_stats:
                track_stats_json = json.dumps(breeder.track_stats, ensure_ascii=False)
                
            if breeder.distance_stats:
                distance_stats_json = json.dumps(breeder.distance_stats, ensure_ascii=False)
                
            if breeder.produced_horses:
                produced_horses_json = json.dumps(breeder.produced_horses, ensure_ascii=False)
                
            if breeder.stallion_stats:
                stallion_stats_json = json.dumps(breeder.stallion_stats, ensure_ascii=False)
            
            breeder_data = {
                'breeder_id': breeder.breeder_id,
                'name_ja': breeder.name_ja,
                'name_en': breeder.name_en,
                'established_date': breeder.established_date,
                'location': breeder.location,
                'breeder_type': breeder.breeder_type,
                'status': breeder.status,
                'total_races': breeder.total_races,
                'wins': breeder.wins,
                'seconds': breeder.seconds,
                'thirds': breeder.thirds,
                'win_rate': breeder.win_rate,
                'second_rate': breeder.second_rate,
                'show_rate': breeder.show_rate,
                'total_prize_money': breeder.total_prize_money,
                'total_horses_produced': breeder.total_horses_produced,
                'active_horses': breeder.active_horses,
                'retired_horses': breeder.retired_horses,
                'stakes_wins': breeder.stakes_wins,
                'grade1_wins': breeder.grade1_wins,
                'debut_horses': breeder.debut_horses,
                'yearly_stats': yearly_stats_json,
                'race_stats': race_stats_json,
                'track_stats': track_stats_json,
                'distance_stats': distance_stats_json,
                'produced_horses': produced_horses_json,
                'stallion_stats': stallion_stats_json,
                'created_at': breeder.created_at or datetime.now(),
                'updated_at': breeder.updated_at or datetime.now()
            }
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO breeders (
                            breeder_id, name_ja, name_en, established_date, location, breeder_type,
                            status, total_races, wins, seconds, thirds,
                            win_rate, second_rate, show_rate, total_prize_money,
                            total_horses_produced, active_horses, retired_horses, stakes_wins, grade1_wins, debut_horses,
                            yearly_stats, race_stats, track_stats, distance_stats, produced_horses, stallion_stats,
                            created_at, updated_at
                        ) VALUES (
                            %(breeder_id)s, %(name_ja)s, %(name_en)s, %(established_date)s, %(location)s, %(breeder_type)s,
                            %(status)s, %(total_races)s, %(wins)s, %(seconds)s, %(thirds)s,
                            %(win_rate)s, %(second_rate)s, %(show_rate)s, %(total_prize_money)s,
                            %(total_horses_produced)s, %(active_horses)s, %(retired_horses)s, %(stakes_wins)s, %(grade1_wins)s, %(debut_horses)s,
                            %(yearly_stats)s::jsonb, %(race_stats)s::jsonb, %(track_stats)s::jsonb, %(distance_stats)s::jsonb, %(produced_horses)s::jsonb, %(stallion_stats)s::jsonb,
                            %(created_at)s, %(updated_at)s
                        )
                        ON CONFLICT (breeder_id) DO UPDATE SET
                            name_ja = EXCLUDED.name_ja,
                            name_en = EXCLUDED.name_en,
                            established_date = EXCLUDED.established_date,
                            location = EXCLUDED.location,
                            breeder_type = EXCLUDED.breeder_type,
                            status = EXCLUDED.status,
                            total_races = EXCLUDED.total_races,
                            wins = EXCLUDED.wins,
                            seconds = EXCLUDED.seconds,
                            thirds = EXCLUDED.thirds,
                            win_rate = EXCLUDED.win_rate,
                            second_rate = EXCLUDED.second_rate,
                            show_rate = EXCLUDED.show_rate,
                            total_prize_money = EXCLUDED.total_prize_money,
                            total_horses_produced = EXCLUDED.total_horses_produced,
                            active_horses = EXCLUDED.active_horses,
                            retired_horses = EXCLUDED.retired_horses,
                            stakes_wins = EXCLUDED.stakes_wins,
                            grade1_wins = EXCLUDED.grade1_wins,
                            debut_horses = EXCLUDED.debut_horses,
                            yearly_stats = EXCLUDED.yearly_stats,
                            race_stats = EXCLUDED.race_stats,
                            track_stats = EXCLUDED.track_stats,
                            distance_stats = EXCLUDED.distance_stats,
                            produced_horses = EXCLUDED.produced_horses,
                            stallion_stats = EXCLUDED.stallion_stats,
                            updated_at = EXCLUDED.updated_at
                    """, breeder_data)
                    conn.commit()
                    logger.debug(f"Breeder inserted/updated: {breeder.breeder_id} ({breeder.name_ja})")
                    return True
        except Exception as e:
            logger.error(f"Error inserting breeder {breeder.breeder_id}: {e}")
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