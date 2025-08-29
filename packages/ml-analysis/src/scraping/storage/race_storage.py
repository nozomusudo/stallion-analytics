"""
レースデータのSupabaseストレージ操作
src/scraping/storage/race_storage.py
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from ..storage.supabase_storage import SupabaseStorage
from ...database.schemas.race_schema import Race, RaceResult

logger = logging.getLogger(__name__)

class RaceStorage(SupabaseStorage):
    """レースデータのSupabaseストレージクラス"""
    
    def __init__(self):
        super().__init__()
    
    def insert_race(self, race: Race) -> bool:
        """単一レースデータの挿入"""
        try:
            data = race.to_dict()
            result = self.client.table('races').insert(data).execute()
            
            if result.data:
                logger.info(f"Race inserted successfully: {race.race_id}")
                return True
            else:
                logger.error(f"Failed to insert race: {race.race_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error inserting race {race.race_id}: {str(e)}")
            return False
    
    def insert_race_results(self, results: List[RaceResult]) -> Tuple[int, int]:
        """レース結果の一括挿入
        
        Returns:
            Tuple[int, int]: (成功件数, 失敗件数)
        """
        if not results:
            return 0, 0
            
        success_count = 0
        error_count = 0
        
        # バッチサイズごとに分割して挿入
        batch_size = 50
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            
            try:
                data_batch = [result.to_dict() for result in batch]
                result = self.client.table('race_results').insert(data_batch).execute()
                
                if result.data:
                    success_count += len(batch)
                    logger.info(f"Batch inserted: {len(batch)} results")
                else:
                    error_count += len(batch)
                    logger.error(f"Failed to insert batch of {len(batch)} results")
                    
            except Exception as e:
                error_count += len(batch)
                race_id = batch[0].race_id if batch else "unknown"
                logger.error(f"Error inserting batch for race {race_id}: {str(e)}")
        
        logger.info(f"Race results insertion complete: {success_count} success, {error_count} failed")
        return success_count, error_count
    
    def insert_complete_race_data(self, race: Race, results: List[RaceResult]) -> bool:
        """レース基本情報と結果を一括挿入（トランザクション風）"""
        try:
            # 1. レース基本情報を挿入
            race_success = self.insert_race(race)
            if not race_success:
                logger.error(f"Failed to insert race data: {race.race_id}")
                return False
            
            # 2. レース結果を挿入
            success_count, error_count = self.insert_race_results(results)
            
            if error_count > 0:
                logger.warning(f"Some race results failed to insert: {error_count} errors")
            
            # 成功率が80%以上なら成功とみなす
            total_results = len(results)
            success_rate = success_count / total_results if total_results > 0 else 1.0
            
            if success_rate >= 0.8:
                logger.info(f"Race data insertion completed successfully: {race.race_id}")
                return True
            else:
                logger.error(f"Too many failures in race results insertion: {race.race_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error in complete race data insertion: {str(e)}")
            return False
    
    def check_race_exists(self, race_id: str) -> bool:
        """レースが既に存在するかチェック"""
        try:
            result = self.client.table('races').select('race_id').eq('race_id', race_id).execute()
            return len(result.data) > 0
        except Exception as e:
            logger.error(f"Error checking race existence {race_id}: {str(e)}")
            return False
    
    def get_races_by_date_range(self, start_date: str, end_date: str, grade: Optional[str] = None) -> List[Dict[str, Any]]:
        """日付範囲でレースを取得"""
        try:
            query = self.client.table('races').select('*').gte('race_date', start_date).lte('race_date', end_date)
            
            if grade:
                query = query.eq('grade', grade)
            
            result = query.order('race_date', desc=True).execute()
            return result.data
            
        except Exception as e:
            logger.error(f"Error fetching races by date range: {str(e)}")
            return []
    
    def get_race_results(self, race_id: str) -> List[Dict[str, Any]]:
        """特定レースの結果を取得"""
        try:
            result = self.client.table('race_results').select('*').eq('race_id', race_id).order('finish_position').execute()
            return result.data
        except Exception as e:
            logger.error(f"Error fetching race results for {race_id}: {str(e)}")
            return []
    
    def get_race_summary(self, limit: int = 100) -> List[Dict[str, Any]]:
        """レースサマリービューからデータを取得"""
        try:
            result = self.client.table('race_summary').select('*').limit(limit).execute()
            return result.data
        except Exception as e:
            logger.error(f"Error fetching race summary: {str(e)}")
            return []
    
    def get_horse_race_history(self, horse_id: str) -> List[Dict[str, Any]]:
        """特定の馬のレース履歴を取得"""
        try:
            result = self.client.table('race_results')\
                .select('*, races!inner(*)')\
                .eq('horse_id', horse_id)\
                .order('races.race_date', desc=True)\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f"Error fetching horse race history for {horse_id}: {str(e)}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """データベースの統計情報を取得"""
        try:
            stats = {}
            
            # レース数
            races_result = self.client.table('races').select('race_id', count='exact').execute()
            stats['total_races'] = races_result.count
            
            # 結果数
            results_result = self.client.table('race_results').select('result_id', count='exact').execute()
            stats['total_results'] = results_result.count
            
            # グレード別レース数
            grade_result = self.client.table('races').select('grade').execute()
            grade_counts = {}
            for race in grade_result.data:
                grade = race.get('grade', 'Unknown')
                grade_counts[grade] = grade_counts.get(grade, 0) + 1
            stats['races_by_grade'] = grade_counts
            
            # 最新レース日
            latest_result = self.client.table('races').select('race_date').order('race_date', desc=True).limit(1).execute()
            if latest_result.data:
                stats['latest_race_date'] = latest_result.data[0]['race_date']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error fetching statistics: {str(e)}")
            return {}
    
    def delete_race_and_results(self, race_id: str) -> bool:
        """レースとその結果を削除（開発・テスト用）"""
        try:
            # CASCADE設定により、race_resultsも自動削除される
            result = self.client.table('races').delete().eq('race_id', race_id).execute()
            
            if result.data:
                logger.info(f"Race and its results deleted: {race_id}")
                return True
            else:
                logger.warning(f"No race found to delete: {race_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting race {race_id}: {str(e)}")
            return False

# 使用例とテスト用コード
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    from database.schemas.race_schema import Race, RaceResult
    from datetime import date, time
    from decimal import Decimal
    
    # ログ設定
    logging.basicConfig(level=logging.INFO)
    
    storage = RaceStorage()
    
    # 統計情報表示
    stats = storage.get_statistics()
    print("=== Database Statistics ===")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # サンプルデータでテスト（必要に応じてコメントアウト）
    """
    sample_race = Race(
        race_id="TEST000001",
        race_date=date(2025, 8, 24),
        track_name="東京",
        race_number=11,
        race_name="テストレース",
        grade="G1",
        distance=2000,
        track_type="芝",
        weather="晴",
        track_condition="良",
        total_horses=10
    )
    
    sample_results = [
        RaceResult(
            race_id="TEST000001",
            horse_id="TEST_HORSE_001",
            horse_name="テスト馬1",
            finish_position=1,
            bracket_number=1,
            horse_number=1,
            age=3,
            sex="牡",
            jockey_weight=Decimal("57.0"),
            jockey_name="テスト騎手1",
            trainer_name="テスト調教師1"
        )
    ]
    
    # データ挿入テスト
    success = storage.insert_complete_race_data(sample_race, sample_results)
    print(f"Insert test result: {success}")
    
    # データ取得テスト
    if success:
        race_data = storage.get_race_results("TEST000001")
        print("Retrieved race results:", race_data)
        
        # テストデータ削除
        storage.delete_race_and_results("TEST000001")
    """