"""
レーススクレイパーメインクラス
src/scraping/scrapers/race_scraper.py
"""

import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from urllib.parse import urljoin
import math

import requests
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from ..extractors.race.race_list_extractor import RaceListExtractor
from ..extractors.race.race_detail_extractor import RaceDetailExtractor
from ..storage.race_storage import RaceStorage
from ...database.schemas.race_schema import Race, RaceResult, RaceDataValidator, RacePayout

logger = logging.getLogger(__name__)

class RaceScraper(BaseScraper):
    """レースデータスクレイパー"""
    
    def __init__(self, delay: float = 1.0):
        super().__init__(delay)
        self.delay = delay  # 明示的に設定
        self.base_url = "https://db.netkeiba.com"
        self.list_extractor = RaceListExtractor()
        self.detail_extractor = RaceDetailExtractor()
        self.storage = RaceStorage()
        self.validator = RaceDataValidator()
    
    def scrape(self, target: str, **kwargs) -> Dict[str, Any]:
        """
        抽象メソッドscrapeの実装
        
        Args:
            target: スクレイピング対象 ('list', 'detail', 'year')
            **kwargs: 追加パラメータ
            
        Returns:
            Dict: スクレイピング結果
        """
        if target == 'list':
            return self._scrape_list(**kwargs)
        elif target == 'detail':
            return self._scrape_detail(**kwargs)
        elif target == 'year':
            return self._scrape_year(**kwargs)
        else:
            raise ValueError(f"Unknown target: {target}")
    
    def _scrape_list(self, **kwargs) -> Dict[str, Any]:
        """レース一覧をスクレイピング"""
        race_list = self.scrape_race_list_by_conditions(**kwargs)
        return {'races': race_list, 'count': len(race_list)}
    
    def _scrape_detail(self, race_id: str, **kwargs) -> Dict[str, Any]:
        """レース詳細をスクレイピング"""
        result = self.scrape_race_detail(race_id)
        if result:
            race, results = result
            return {'race': race, 'results': results, 'success': True}
        return {'success': False}
    
    def _scrape_year(self, year: int, **kwargs) -> Dict[str, Any]:
        """年度単位でスクレイピング"""
        stats = self.scrape_g1_races_by_year(year)
        return stats
        
    def scrape_race_list_by_conditions(self, 
                                     start_year: int = 2024,
                                     end_year: int = 2025,
                                     grades: List[str] = ['1', '2'],  # G1, G2
                                     track_types: List[str] = ['1'],  # コース種別: 1=芝, 2=ダート
                                     venue_types: List[str] = ['jra'],  # 競馬場種別: 'jra', 'local', 'all'
                                     limit: int = 100) -> List[Dict[str, Any]]:
        """
        条件指定でレース一覧を取得
        https://db.netkeiba.com/?pid=race_list&word=
            &start_year=2010&start_mon=none&end_year=2018&end_mon=none
            &list=100&page=2
            &sort=date&track%5B%5D=1
            &jyo%5B%5D=01&jyo%5B%5D=02&jyo%5B%5D=03&jyo%5B%5D=04&jyo%5B%5D=05
            &jyo%5B%5D=06&jyo%5B%5D=07&jyo%5B%5D=08&jyo%5B%5D=09&jyo%5B%5D=10
            &grade%5B%5D=1
            &page=2
        
        Args:
            start_year: 開始年
            end_year: 終了年  
            grades: グレード ['1'=G1, '2'=G2, '3'=G3]
            tracks: トラック ['1'=中央競馬]
            limit: 取得件数上限
            
        Returns:
            List[Dict]: レース基本情報のリスト
        """

        # 中央競馬場のコード定数
        JRA_TRACKS = [
            '01',  # 札幌
            '02',  # 函館
            '03',  # 福島
            '04',  # 新潟
            '05',  # 東京
            '06',  # 中山
            '07',  # 中京
            '08',  # 京都
            '09',  # 阪神
            '10'   # 小倉
        ]

        # 地方競馬場のコード定数
        LOCAL_TRACKS = [
            '30',  # 門別
            '35',  # 盛岡
            '36',  # 水沢
            '42',  # 浦和
            '43',  # 船橋
            '44',  # 大井
            '45',  # 川崎
            '46',  # 金沢
            '47',  # 笠松
            '48',  # 名古屋
            '50',  # 園田
            '51',  # 姫路
            '54',  # 高知
            '55',  # 佐賀
            '65'   # 帯広(ば)
        ]

        """競馬場種別から競馬場コードリストを生成"""
        track_codes = []
        if 'jra' in venue_types or 'all' in venue_types:
            track_codes.extend(JRA_TRACKS)
        if 'local' in venue_types or 'all' in venue_types:
            track_codes.extend(LOCAL_TRACKS)
        
        # listパラメータは100で固定（最大効率）
        list_size = 100
        all_races = []
        
        # URL構築
        base_list_url = f"{self.base_url}/?pid=race_list"
        
        base_params = {
            'word': '',
            'start_year': start_year,
            'start_mon': 'none', 
            'end_year': end_year,
            'end_mon': 'none',
            'list': list_size,
            'sort': 'date',
            'track[]': track_types,        # リストとして渡す
            'jyo[]': track_codes,      # リストとして渡す
            'grade[]': grades         # リストとして渡す
        }
        
        try:
            logger.info(f"Fetching race list: {start_year}-{end_year}, grades: {grades}, limit: {limit}")
            
            page = 1
            while len(all_races) < limit:
                # ページパラメータを追加
                params = base_params.copy()
                params['page'] = page
                
                logger.info(f"Fetching page {page}...")
                
                response = self.session.get(base_list_url, params=params, timeout=30)
                response.raise_for_status()
                response.encoding = 'euc-jp'
                
                logger.debug(f"Page {page} URL: {response.url}")
                logger.debug(f"Page {page} status: {response.status_code}")
                
                # レース一覧を抽出
                page_races = self.list_extractor.extract_race_list(response.text)
                
                if not page_races:
                    logger.info(f"No races found on page {page} - reached end of results")
                    break
                
                logger.info(f"Page {page}: Found {len(page_races)} races")
                all_races.extend(page_races)
                
                # limitに達したら終了
                if len(all_races) >= limit:
                    all_races = all_races[:limit]  # 超過分を削除
                    break
                
                page += 1
                
                # レート制限
                time.sleep(self.delay)
            
            logger.info(f"Total races collected: {len(all_races)}")
            return all_races
            
        except Exception as e:
            logger.error(f"Error fetching race list: {str(e)}")
            return all_races  # エラー時もそれまでに取得したデータを返す
    
    def scrape_race_detail(self, race_id: str) -> Optional[Tuple[Race, List[RaceResult], List[RacePayout]]]:
        """
        レース詳細データを取得
        
        Args:
            race_id: レースID
            
        Returns:
            Tuple[Race, List[RaceResult], List[RacePayout]]: レース基本情報、結果、払い戻しのタプル
        """
        
        detail_url = f"{self.base_url}/race/{race_id}/"
        logger.info(f"race detail url: {detail_url}")
        
        try:
            logger.debug(f"Fetching race detail: {race_id}")
            
            response = self.session.get(detail_url, timeout=30)
            response.raise_for_status()
            response.encoding = 'euc-jp'

            # logger.info(f"extract_race_detailに送る前のrace_scraperでの処理です！rsponse=: {response.text[:1000]}")  # 最初の1000文字だけ表示
            
            # レース詳細を抽出（払い戻し情報も含む）
            race_data = self.detail_extractor.extract_race_detail(response.text, race_id)
            
            if not race_data:
                logger.warning(f"No race data extracted for {race_id}")
                return None
                
            race, results, payouts = race_data
            
            # データバリデーション
            race_errors = self.validator.validate_race(race)
            if race_errors:
                logger.warning(f"Race validation errors for {race_id}: {race_errors}")
            
            for result in results:
                result_errors = self.validator.validate_race_result(result)
                if result_errors:
                    logger.warning(f"Result validation errors for {result.horse_name}: {result_errors}")
            
            # 払い戻しデータのバリデーション
            for payout in payouts:
                payout_errors = self.validator.validate_race_payout(payout)
                if payout_errors:
                    logger.warning(f"Payout validation errors for {payout.bet_type} {payout.combination}: {payout_errors}")
            
            # 払い戻しデータの整合性チェック
            if payouts:
                consistency_errors = self.validator.validate_payout_consistency(payouts)
                if consistency_errors:
                    logger.warning(f"Payout consistency errors for {race_id}: {consistency_errors}")
            
            logger.info(f"Successfully extracted race data: {race_id} ({len(results)} horses, {len(payouts)} payouts)")
            return race, results, payouts
            
        except Exception as e:
            logger.error(f"Error fetching race detail {race_id}: {str(e)}")
            return None
    
    def scrape_and_store_races(self, 
                             race_list: List[Dict[str, Any]], 
                             skip_existing: bool = True) -> Dict[str, int]:
        """
        レースリストを処理してデータベースに保存
        
        Args:
            race_list: scrape_race_list_by_conditions()で取得したレースリスト
            skip_existing: 既存レースをスキップするか
            
        Returns:
            Dict[str, int]: 処理結果統計
        """
        
        stats = {
            'total': len(race_list),
            'success': 0,
            'skipped': 0,
            'failed': 0,
            'errors': []
        }
        
        for i, race_info in enumerate(race_list, 1):
            race_id = race_info.get('race_id')
            
            if not race_id:
                logger.warning(f"Missing race_id in race info: {race_info}")
                stats['failed'] += 1
                continue
            
            try:
                # 既存チェック
                if skip_existing and self.storage.check_race_exists(race_id):
                    logger.info(f"Race already exists, skipping: {race_id}")
                    stats['skipped'] += 1
                    continue
                
                # レース詳細を取得
                race_data = self.scrape_race_detail(race_id)
                
                if not race_data:
                    logger.error(f"Failed to scrape race detail: {race_id}")
                    stats['failed'] += 1
                    stats['errors'].append(f"Detail scraping failed: {race_id}")
                    continue
                
                race, results = race_data
                
                # データベースに保存
                success = self.storage.insert_complete_race_data(race, results)
                
                if success:
                    stats['success'] += 1
                    logger.info(f"Race data saved successfully: {race_id} ({i}/{len(race_list)})")
                else:
                    stats['failed'] += 1
                    stats['errors'].append(f"Database insert failed: {race_id}")
                    logger.error(f"Failed to save race data: {race_id}")
                
                # レート制限
                time.sleep(self.delay)
                
            except Exception as e:
                stats['failed'] += 1
                error_msg = f"Unexpected error processing {race_id}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error(error_msg)
                
                # 重大なエラーの場合は少し長めに待機
                time.sleep(self.delay * 2)
        
        # 結果サマリー
        logger.info("=== Scraping Summary ===")
        logger.info(f"Total races: {stats['total']}")
        logger.info(f"Successfully processed: {stats['success']}")
        logger.info(f"Skipped (existing): {stats['skipped']}")
        logger.info(f"Failed: {stats['failed']}")
        
        if stats['errors']:
            logger.warning(f"Errors encountered: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # 最初の5件のみ表示
                logger.warning(f"  - {error}")
        
        return stats
    
    def scrape_g1_races_by_year(self, year: int) -> Dict[str, int]:
        """特定年のG1レースをすべて取得"""
        race_list = self.scrape_race_list_by_conditions(
            start_year=year,
            end_year=year,
            grades=['1'],  # G1のみ
            limit=200
        )
        
        if not race_list:
            logger.warning(f"No G1 races found for year {year}")
            return {'total': 0, 'success': 0, 'skipped': 0, 'failed': 0, 'errors': []}
        
        logger.info(f"Found {len(race_list)} G1 races for {year}")
        return self.scrape_and_store_races(race_list)
    
    def scrape_recent_races(self, days: int = 30) -> Dict[str, int]:
        """直近N日のレースを取得"""
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        race_list = self.scrape_race_list_by_conditions(
            start_year=start_date.year,
            end_year=end_date.year,
            grades=['1', '2'],  # G1, G2
            limit=100
        )
        
        # 日付でフィルタリング（より精密に）
        filtered_races = []
        for race in race_list:
            race_date_str = race.get('race_date')
            if race_date_str:
                try:
                    race_date = datetime.strptime(race_date_str, '%Y-%m-%d').date()
                    if start_date.date() <= race_date <= end_date.date():
                        filtered_races.append(race)
                except ValueError:
                    logger.warning(f"Invalid race date format: {race_date_str}")
        
        logger.info(f"Found {len(filtered_races)} recent races (last {days} days)")
        return self.scrape_and_store_races(filtered_races)

# 使用例とテスト用コード
if __name__ == "__main__":
    import sys
    import os
    
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    scraper = RaceScraper(delay=1.0)
    
    # テスト: 2024年G1レースリストを取得（実際のスクレイピングは行わない）
    print("=== Testing Race List Extraction ===")
    race_list = scraper.scrape_race_list_by_conditions(
        start_year=2010,
        end_year=2012,
        grades=['1'],
        limit=20
    )
    
    print(f"Found {len(race_list)} races")
    for race in race_list[:3]:  # 最初の3件のみ表示
        print(f"  - {race}")
    
    # 実際のデータ取得・保存を行う場合（コメントアウトして使用）
    """
    print("=== Starting G1 Race Scraping ===")
    stats = scraper.scrape_g1_races_by_year(2024)
    print("Final stats:", stats)
    """