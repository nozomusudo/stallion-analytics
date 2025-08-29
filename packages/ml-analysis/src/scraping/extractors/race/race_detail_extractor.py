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
            # レース名とレース番号を取得 (dl.racedata.fc)
            race_data_dl = soup.find('dl', class_='racedata fc')
            if not race_data_dl:
                logger.error(f"Race data section not found for {race_id}")
                return None
            
            # レース名 (h1)
            race_name_h1 = race_data_dl.find('h1')
            if not race_name_h1:
                logger.error(f"Race name not found for {race_id}")
                return None
            race_name = race_name_h1.text.strip()
            
            # レース番号 (dt)
            race_number_dt = race_data_dl.find('dt')
            race_number = self._extract_race_number(race_number_dt.text.strip()) if race_number_dt else 1
            
            # グレードを抽出
            grade = self._extract_grade_from_name(race_name)
            
            # レース条件を抽出 (レース詳細情報のpタグから)
            race_conditions = self._extract_race_conditions(race_data_dl)
            
            # 開催情報を抽出 (p.smalltxt)
            venue_info = self._extract_venue_info(soup)
            
            # 結果テーブルから追加情報を抽出
            additional_info = self._extract_additional_race_info(soup)
            
            # Raceオブジェクトを構築
            race = Race(
                race_id=race_id,
                race_date=venue_info.get('race_date') or date.today(),
                track_name=venue_info.get('track_name', ''),
                race_number=race_number,
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

            # logger.info(f"RACE: {race}")
            
            return race
            
        except Exception as e:
            logger.error(f"Error extracting race info for {race_id}: {str(e)}")
            return None
    
    def _extract_race_results(self, soup: BeautifulSoup, race_id: str) -> List[RaceResult]:
        """レース結果を抽出"""
        results = []
        
        try:
            # 結果テーブルを取得 (table.race_table_01)
            result_table = soup.find('table', class_='race_table_01')
            
            if not result_table:
                logger.warning(f"Result table not found for {race_id}")
                return results
            
            # テーブルの行を取得（ヘッダー行は除く）
            rows = result_table.find_all('tr')[1:]
            
            for row in rows:
                try:
                    result = self._extract_race_result_row(row, race_id)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f"Error extracting result row for {race_id}: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(results)} race results for {race_id}")
            # logger.info(f"First 3 results: {results[:3]}")
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
            
            # 基本情報
            finish_position = self._parse_int(cells[0].text.strip())
            bracket_number = self._parse_int(cells[1].text.strip())
            horse_number = self._parse_int(cells[2].text.strip())
            
            # 馬情報
            horse_elem = cells[3].find('a')
            horse_name = horse_elem.text.strip() if horse_elem else cells[3].text.strip()
            horse_id = self._extract_id_from_url(horse_elem.get('href', ''), 'horse') if horse_elem else None
            
            # 性齢
            sex_age = cells[4].text.strip()
            sex, age = self._parse_sex_age(sex_age)
            
            # 斤量
            jockey_weight = self._parse_decimal(cells[5].text.strip())
            
            # 騎手
            jockey_elem = cells[6].find('a')
            jockey_name = jockey_elem.text.strip() if jockey_elem else cells[6].text.strip()
            jockey_id = self._extract_id_from_url(jockey_elem.get('href', ''), 'jockey') if jockey_elem else None
            
            # レース結果
            race_time = cells[7].text.strip()
            time_diff = cells[8].text.strip()
            
            # 通過順位、上り3ハロン
            passing_order = cells[10].text.strip() if len(cells) > 10 else None
            last_3f = self._parse_decimal(cells[11].text.strip()) if len(cells) > 11 else None
            
            # オッズ・人気
            odds = self._parse_decimal(cells[12].text.strip()) if len(cells) > 12 else None
            popularity = self._parse_int(cells[13].text.strip()) if len(cells) > 13 else None
            
            # 馬体重
            horse_weight, weight_change = self._parse_horse_weight(cells[14].text.strip()) if len(cells) > 14 else (None, None)
            
            # 調教師（西部、東部の表記も含む）
            trainer_name, trainer_region, trainer_id = self._extract_trainer_info(cells, 18)
            
            # 馬主
            owner_name, owner_id = self._extract_owner_info(cells, 19)
            
            # 賞金
            prize_money = self._parse_decimal(cells[20].text.strip()) if len(cells) > 20 else None
            
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
    
    # === 抽出ヘルパーメソッド ===
    
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
        """レース条件を抽出（距離、コース、天候、馬場状態、発走時刻）"""
        conditions = {}
        
        try:
            condition_text = racedata_elem.get_text()
            
            # 距離とコース種別 - "芝右 外1600m"のような形式
            distance_match = re.search(r'(芝|ダート?)([左右直線]*?)\s*(?:外|内)?(\d+)m', condition_text)
            if distance_match:
                track_type = distance_match.group(1)
                if track_type.startswith('ダ'):
                    track_type = 'ダート'
                conditions['track_type'] = track_type
                conditions['track_direction'] = distance_match.group(2) or None
                conditions['distance'] = int(distance_match.group(3))
            
            # 天候 - "天候 : 曇"
            weather_match = re.search(r'天候\s*[:：]\s*([^\s/&]+)', condition_text)
            if weather_match:
                conditions['weather'] = weather_match.group(1).strip()
            
            # 馬場状態 - "芝 : 良"
            track_condition_match = re.search(r'(芝|ダート?)\s*[:：]\s*([^\s/&]+)', condition_text)
            if track_condition_match:
                conditions['track_condition'] = track_condition_match.group(2).strip()
            
            # 発走時刻 - "発走 : 15:40"
            time_match = re.search(r'発走\s*[:：]\s*(\d{1,2}):(\d{2})', condition_text)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
                conditions['start_time'] = time(hour, minute)
                
        except Exception as e:
            logger.warning(f"Error extracting race conditions: {str(e)}")
        
        return conditions
    
    def _extract_race_number(self, race_num_text: str) -> int:
        """レース番号を抽出 - "11 R"のような形式"""
        try:
            match = re.search(r'(\d+)\s*R', race_num_text)
            if match:
                return int(match.group(1))
        except Exception:
            pass
        return 1
    
    def _extract_venue_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """開催情報を抽出（日付、競馬場）"""
        info = {}
        
        try:
            # "2024年12月08日 7回京都4日目"のような形式を探す
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
        """追加のレース情報を抽出（出走頭数、勝ちタイムなど）"""
        info = {}
        
        try:
            result_table = soup.find('table', class_='race_table_01')
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
    
    def _extract_trainer_info(self, cells: List, index: int) -> Tuple[str, Optional[str], Optional[str]]:
        """調教師情報を抽出"""
        trainer_name = ""
        trainer_region = None
        trainer_id = None
        
        if len(cells) > index:
            trainer_cell = cells[index]
            trainer_elem = trainer_cell.find('a')
            if trainer_elem:
                trainer_name = trainer_elem.text.strip()
                trainer_id = self._extract_id_from_url(trainer_elem.get('href', ''), 'trainer')
            
            # 地域を抽出 - [西]や[東]
            region_match = re.search(r'\[(東|西)\]', trainer_cell.text)
            if region_match:
                trainer_region = region_match.group(1)
                
        return trainer_name, trainer_region, trainer_id
    
    def _extract_owner_info(self, cells: List, index: int) -> Tuple[str, Optional[str]]:
        """馬主情報を抽出"""
        owner_name = ""
        owner_id = None
        
        if len(cells) > index:
            owner_cell = cells[index]
            owner_elem = owner_cell.find('a')
            if owner_elem:
                owner_name = owner_elem.text.strip()
                owner_id = self._extract_id_from_url(owner_elem.get('href', ''), 'owner')
                
        return owner_name, owner_id
    
    # === パースヘルパーメソッド ===
    
    def _parse_int(self, text: str) -> Optional[int]:
        """安全に整数をパース"""
        try:
            return int(text.strip()) if text.strip() else None
        except (ValueError, AttributeError):
            return None
    
    def _parse_decimal(self, text: str) -> Optional[Decimal]:
        """安全にDecimalをパース"""
        try:
            cleaned_text = text.strip().replace(',', '')
            return Decimal(cleaned_text) if cleaned_text else None
        except (InvalidOperation, AttributeError, ValueError):
            return None
    
    def _parse_sex_age(self, sex_age: str) -> Tuple[Optional[str], Optional[int]]:
        """性齢をパース - "牝2" -> ("牝", 2)"""
        try:
            match = re.match(r'([牡牝セ])(\d+)', sex_age.strip())
            if match:
                sex = match.group(1)
                age = int(match.group(2))
                return sex, age
        except Exception:
            pass
        return None, None
    
    def _parse_horse_weight(self, weight_text: str) -> Tuple[Optional[int], Optional[int]]:
        """馬体重をパース - "484(0)" -> (484, 0)"""
        try:
            match = re.match(r'(\d+)\(([+-]?\d+)\)', weight_text.strip())
            if match:
                weight = int(match.group(1))
                change = int(match.group(2))
                return weight, change
        except Exception:
            pass
        return None, None
    
    def _extract_id_from_url(self, href: str, entity_type: str) -> Optional[str]:
        """URLからIDを抽出する統一メソッド"""
        try:
            if entity_type == 'horse':
                match = re.search(r'/horse/(\d+)/?', href)
            elif entity_type == 'jockey':
                match = re.search(r'/jockey/result/recent/(\d+)/?', href)
            elif entity_type == 'trainer':
                match = re.search(r'/trainer/result/recent/(\d+)/?', href)
            elif entity_type == 'owner':
                match = re.search(r'/owner/result/recent/(\d+)/?', href)
            else:
                return None
                
            if match:
                return match.group(1)
        except Exception:
            pass
        return None


# 使用例とテスト用コード
if __name__ == "__main__":
    import requests
    
    logging.basicConfig(level=logging.INFO)
    
    extractor = RaceDetailExtractor()
    
    print("=== Race Detail Extractor Test ===")
    
    # テスト用URL
    test_url = "https://db.netkeiba.com/race/202408070411/"
    race_id = "202408070411"
    
    try:
        # HTMLを取得
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(test_url, headers=headers)
        response.raise_for_status()
        response.encoding = 'euc-jp'
        
        print(f"Successfully fetched HTML from {test_url}")
        
        # データを抽出
        result = extractor.extract_race_detail(response.text, race_id)
        
        if result:
            race, race_results = result
            print(f"\n=== Race Info ===")
            print(f"Race Name: {race.race_name}")
            print(f"Date: {race.race_date}")
            print(f"Track: {race.track_name}")
            print(f"Distance: {race.distance}m ({race.track_type})")
            print(f"Total Horses: {race.total_horses}")
            
            print(f"\n=== Race Results (Top 5) ===")
            for i, result in enumerate(race_results[:5]):
                print(f"{result.finish_position}着: {result.horse_name} ({result.jockey_name})")
                
        else:
            print("Failed to extract race data")
            
    except Exception as e:
        print(f"Error: {str(e)}")