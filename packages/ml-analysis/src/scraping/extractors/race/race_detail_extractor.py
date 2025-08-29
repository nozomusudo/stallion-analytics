"""
レース詳細ページからのデータ抽出
src/scraping/extractors/race/race_detail_extractor.py
"""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, time
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup

from ....database.schemas.race_schema import Race, RaceResult

logger = logging.getLogger(__name__)

class RaceDetailExtractor:
    """レース詳細ページからレース情報と結果を抽出するクラス"""
    
    def __init__(self):
        pass
    
    def extract_race_detail(self, html: str, race_id: str) -> Optional[Tuple[Race, List[RaceResult]]]:
        """
        レース詳細ページのHTMLからレース情報と結果を抽出
        
        Args:
            html: レース詳細ページのHTML
            race_id: レースID
            
        Returns:
            Tuple[Race, List[RaceResult]]: レース基本情報と結果のタプル
        """
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # レース基本情報を抽出
            race = self._extract_race_info(soup, race_id)
            if not race:
                logger.error(f"Failed to extract race info for {race_id}")
                return None
            
            # レース結果を抽出
            results = self._extract_race_results(soup, race_id)
            if not results:
                logger.warning(f"No race results found for {race_id}")
                return None
            
            logger.info(f"Successfully extracted race data: {race_id} ({len(results)} horses)")
            return race, results
            
        except Exception as e:
            logger.error(f"Error parsing race detail HTML for {race_id}: {str(e)}")
            return None
    
    def _extract_race_info(self, soup: BeautifulSoup, race_id: str) -> Optional[Race]:
        """レース基本情報を抽出"""
        
        try:
            # レース名を取得 - <h1>要素から
            race_name_elem = soup.find('h1')
            if not race_name_elem:
                logger.error(f"Race name not found for {race_id}")
                return None
            
            race_name = race_name_elem.text.strip()
            grade = self._extract_grade_from_name(race_name)
            
            # レース条件を取得 - racedata要素から
            racedata_elem = soup.find('dl', class_='racedata')
            if not racedata_elem:
                logger.error(f"Race data section not found for {race_id}")
                return None
            
            # 距離、コース、天候、馬場状態を抽出
            race_conditions = self._extract_race_conditions(racedata_elem)
            
            # レース番号を取得
            race_number = self._extract_race_number(soup)
            
            # 開催情報を取得
            venue_info = self._extract_venue_info(soup)
            
            # レース詳細情報を取得
            additional_info = self._extract_additional_race_info(soup)
            
            # Raceオブジェクトを構築
            race = Race(
                race_id=race_id,
                race_date=venue_info.get('race_date') or date.today(),
                track_name=venue_info.get('track_name', ''),
                race_number=race_number or 1,
                race_name=race_name,
                grade=grade,
                distance=race_conditions.get('distance', 0),
                track_type=race_conditions.get('track_type', ''),
                track_direction=race_conditions.get('track_direction'),
                weather=race_conditions.get('weather'),
                track_condition=race_conditions.get('track_condition'),
                start_time=race_conditions.get('start_time'),
                total_horses=additional_info.get('total_horses', 0),
                winning_time=additional_info.get('winning_time'),
                pace=additional_info.get('pace'),
                race_class=additional_info.get('race_class'),
                race_conditions=additional_info.get('race_conditions')
            )
            
            return race
            
        except Exception as e:
            logger.error(f"Error extracting race info for {race_id}: {str(e)}")
            return None
    
    def _extract_race_results(self, soup: BeautifulSoup, race_id: str) -> List[RaceResult]:
        """レース結果を抽出"""
        
        results = []
        
        try:
            # 結果テーブルを探す
            result_table = soup.find('table')  # 最初のテーブルが結果テーブルと仮定
            
            if not result_table:
                logger.warning(f"Result table not found for {race_id}")
                return results
            
            # テーブルの行を取得（ヘッダー行は除く）
            rows = result_table.find('tbody').find_all('tr') if result_table.find('tbody') else result_table.find_all('tr')[1:]
            
            for row in rows:
                try:
                    result = self._extract_race_result_row(row, race_id)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"Error extracting result row for {race_id}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(results)} race results for {race_id}")
            return results
            
        except Exception as e:
            logger.error(f"Error extracting race results for {race_id}: {str(e)}")
            return results
    
    def _extract_race_result_row(self, row, race_id: str) -> Optional[RaceResult]:
        """単一の結果行からRaceResultを抽出"""
        
        try:
            cells = row.find_all('td')
            
            if len(cells) < 15:  # 最低限必要なセル数
                return None
            
            # 各セルからデータを抽出
            finish_position = self._parse_int(cells[0].text.strip())
            bracket_number = self._parse_int(cells[1].text.strip())
            horse_number = self._parse_int(cells[2].text.strip())
            
            # 馬名とID
            horse_elem = cells[3].find('a')
            horse_name = horse_elem.text.strip() if horse_elem else cells[3].text.strip()
            horse_id = self._extract_horse_id(horse_elem.get('href', '')) if horse_elem else None
            
            # 性齢 - "牝3"のような形式
            sex_age = cells[4].text.strip()
            sex, age = self._parse_sex_age(sex_age)
            
            # 斤量
            jockey_weight = self._parse_decimal(cells[5].text.strip())
            
            # 騎手
            jockey_elem = cells[6].find('a')
            jockey_name = jockey_elem.text.strip() if jockey_elem else cells[6].text.strip()
            jockey_id = self._extract_jockey_id(jockey_elem.get('href', '')) if jockey_elem else None
            
            # タイム
            race_time = cells[7].text.strip()
            
            # 着差
            time_diff = cells[8].text.strip()
            
            # 通過順位（存在する場合）
            passing_order = cells[10].text.strip() if len(cells) > 10 else None
            
            # 上り3ハロン
            last_3f = self._parse_decimal(cells[11].text.strip()) if len(cells) > 11 else None
            
            # 単勝オッズ
            odds = self._parse_decimal(cells[12].text.strip()) if len(cells) > 12 else None
            
            # 人気
            popularity = self._parse_int(cells[13].text.strip()) if len(cells) > 13 else None
            
            # 馬体重 - "474(+4)"のような形式
            horse_weight_info = self._parse_horse_weight(cells[14].text.strip()) if len(cells) > 14 else (None, None)
            horse_weight, weight_change = horse_weight_info
            
            # 調教師情報
            trainer_name = ""
            trainer_region = None
            trainer_id = None
            if len(cells) > 18:
                trainer_cell = cells[18]
                trainer_elem = trainer_cell.find('a')
                if trainer_elem:
                    trainer_name = trainer_elem.text.strip()
                    trainer_id = self._extract_trainer_id(trainer_elem.get('href', ''))
                
                # 地域を抽出 - [西]や[東]
                region_match = re.search(r'\[(東|西)\]', trainer_cell.text)
                if region_match:
                    trainer_region = region_match.group(1)
            
            # 馬主
            owner_name = ""
            owner_id = None
            if len(cells) > 19:
                owner_cell = cells[19]
                owner_elem = owner_cell.find('a')
                if owner_elem:
                    owner_name = owner_elem.text.strip()
                    owner_id = self._extract_owner_id(owner_elem.get('href', ''))
            
            # 賞金
            prize_money = None
            if len(cells) > 20:
                prize_money = self._parse_decimal(cells[20].text.strip())
            
            # RaceResultオブジェクトを構築
            result = RaceResult(
                race_id=race_id,
                horse_id=horse_id or f"UNKNOWN_{race_id}_{horse_number}",
                horse_name=horse_name,
                finish_position=finish_position,
                bracket_number=bracket_number or 0,
                horse_number=horse_number or 0,
                age=age or 0,
                sex=sex or "不明",
                jockey_weight=jockey_weight or Decimal('0'),
                jockey_id=jockey_id,
                jockey_name=jockey_name,
                trainer_region=trainer_region,
                trainer_id=trainer_id,
                trainer_name=trainer_name,
                race_time=race_time,
                time_diff=time_diff,
                passing_order=passing_order,
                last_3f=last_3f,
                odds=odds,
                popularity=popularity,
                horse_weight=horse_weight,
                weight_change=weight_change,
                prize_money=prize_money,
                owner_id=owner_id,
                owner_name=owner_name
            )
            
            return result
            
        except Exception as e:
            logger.warning(f"Error extracting race result row: {str(e)}")
            return None
    
    def _extract_grade_from_name(self, race_name: str) -> Optional[str]:
        """レース名からグレードを抽出"""
        if '(GI)' in race_name or '(G1)' in race_name:
            return 'G1'
        elif '(GII)' in race_name or '(G2)' in race_name:
            return 'G2'
        elif '(GIII)' in race_name or '(G3)' in race_name:
            return 'G3'
        return None
    
    def _extract_race_conditions(self, racedata_elem) -> Dict[str, Any]:
        """レース条件を抽出"""
        conditions = {}
        
        try:
            # レース条件のテキストを取得
            condition_text = racedata_elem.get_text()
            
            # 距離とコース種別を抽出 - "芝左2400m"のような形式
            distance_match = re.search(r'(芝|ダート?)([左右直線]*?)(\d+)m', condition_text)
            if distance_match:
                track_type = distance_match.group(1)
                if track_type.startswith('ダ'):
                    track_type = 'ダート'
                conditions['track_type'] = track_type
                conditions['track_direction'] = distance_match.group(2) or None
                conditions['distance'] = int(distance_match.group(3))
            
            # 天候を抽出 - "天候 : 曇"
            weather_match = re.search(r'天候\s*[:：]\s*([^\s/&]+)', condition_text)
            if weather_match:
                conditions['weather'] = weather_match.group(1).strip()
            
            # 馬場状態を抽出 - "芝 : 良" 
            track_condition_match = re.search(r'(芝|ダート?)\s*[:：]\s*([^\s/&]+)', condition_text)
            if track_condition_match:
                conditions['track_condition'] = track_condition_match.group(2).strip()
            
            # 発走時刻を抽出 - "発走 : 15:40"
            time_match = re.search(r'発走\s*[:：]\s*(\d{1,2}):(\d{2})', condition_text)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
                conditions['start_time'] = time(hour, minute)
                
        except Exception as e:
            logger.warning(f"Error extracting race conditions: {str(e)}")
        
        return conditions
    
    def _extract_race_number(self, soup: BeautifulSoup) -> Optional[int]:
        """レース番号を抽出"""
        try:
            # "11 R"のような形式を探す
            race_num_elem = soup.find('dt')
            if race_num_elem:
                race_num_text = race_num_elem.text.strip()
                match = re.search(r'(\d+)\s*R', race_num_text)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return None
    
    def _extract_venue_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """開催情報を抽出"""
        info = {}
        
        try:
            # "2025年05月25日 2回東京10日目"のような形式を探す
            date_elem = soup.find('p', class_='smalltxt')
            if date_elem:
                date_text = date_elem.text
                
                # 日付を抽出
                date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    day = int(date_match.group(3))
                    info['race_date'] = date(year, month, day)
                
                # 競馬場を抽出
                track_match = re.search(r'回([^日]+)\d+日目', date_text)
                if track_match:
                    info['track_name'] = track_match.group(1)
                    
        except Exception as e:
            logger.warning(f"Error extracting venue info: {str(e)}")
        
        return info
    
    def _extract_additional_race_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """追加のレース情報を抽出"""
        info = {}
        
        try:
            # 結果テーブルから出走頭数、勝ちタイムなどを取得
            result_table = soup.find('table')
            if result_table:
                rows = result_table.find_all('tr')
                if rows:
                    info['total_horses'] = len(rows) - 1  # ヘッダー行を除く
                    
                    # 1着の情報から勝ちタイムを取得
                    first_row = rows[1] if len(rows) > 1 else None
                    if first_row:
                        cells = first_row.find_all('td')
                        if len(cells) > 7:
                            info['winning_time'] = cells[7].text.strip()
                            
        except Exception as e:
            logger.warning(f"Error extracting additional race info: {str(e)}")
        
        return info
    
    # ヘルパーメソッド
    def _parse_int(self, text: str) -> Optional[int]:
        """安全に整数をパース"""
        try:
            return int(text.strip())
        except (ValueError, AttributeError):
            return None
    
    def _parse_decimal(self, text: str) -> Optional[Decimal]:
        """安全にDecimalをパース"""
        try:
            # カンマを除去
            cleaned_text = text.strip().replace(',', '')
            return Decimal(cleaned_text)
        except (InvalidOperation, AttributeError, ValueError):
            return None
    
    def _parse_sex_age(self, sex_age: str) -> Tuple[Optional[str], Optional[int]]:
        """性齢をパース"""
        try:
            # "牝3" -> ("牝", 3)
            match = re.match(r'([牡牝セ])(\d+)', sex_age.strip())
            if match:
                sex = match.group(1)
                age = int(match.group(2))
                return sex, age
        except Exception:
            pass
        return None, None
    
    def _parse_horse_weight(self, weight_text: str) -> Tuple[Optional[int], Optional[int]]:
        """馬体重をパース"""
        try:
            # "474(+4)" -> (474, 4)
            # "450(-2)" -> (450, -2)
            match = re.match(r'(\d+)\(([+-]?\d+)\)', weight_text.strip())
            if match:
                weight = int(match.group(1))
                change = int(match.group(2))
                return weight, change
        except Exception:
            pass
        return None, None
    
    def _extract_horse_id(self, href: str) -> Optional[str]:
        """馬URLから馬IDを抽出"""
        try:
            match = re.search(r'/horse/(\d+)/?', href)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def _extract_jockey_id(self, href: str) -> Optional[str]:
        """騎手URLから騎手IDを抽出"""
        try:
            match = re.search(r'/jockey/result/recent/(\d+)/?', href)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def _extract_trainer_id(self, href: str) -> Optional[str]:
        """調教師URLから調教師IDを抽出"""
        try:
            match = re.search(r'/trainer/result/recent/(\d+)/?', href)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def _extract_owner_id(self, href: str) -> Optional[str]:
        """馬主URLから馬主IDを抽出"""
        try:
            match = re.search(r'/owner/result/recent/(\d+)/?', href)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

# 使用例とテスト用コード
if __name__ == "__main__":
    import sys
    import os
    
    logging.basicConfig(level=logging.INFO)
    
    # サンプルHTMLでテスト（実際のHTMLデータで置き換える）
    extractor = RaceDetailExtractor()
    
    print("=== Race Detail Extractor Test ===")
    print("Ready to extract race detail data")
    print("Use with actual HTML content from race detail pages")