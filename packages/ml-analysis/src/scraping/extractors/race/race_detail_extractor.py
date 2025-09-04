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

from ....database.schemas.race_schema import Race, RaceResult, RacePayout

logger = logging.getLogger(__name__)

class RaceDetailExtractor:
    """レース詳細ページからレース情報と結果を抽出するクラス"""
    
    def extract_race_detail(self, html: str, race_id: str) -> Optional[Tuple[Race, List[RaceResult], List[RacePayout]]]:
        """
        レース詳細ページのHTMLからレース情報、結果、払い戻しを抽出
        
        Args:
            html: レース詳細ページのHTML
            race_id: レースID
            
        Returns:
            Tuple[Race, List[RaceResult], List[RacePayout]]: レース基本情報、結果、払い戻しのタプル
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
            
            # 払い戻し情報を抽出
            payouts = self._extract_race_payouts(soup, race_id)
            
            logger.info(f"Successfully extracted race data: {race_id} ({len(results)} horses, {len(payouts)} payouts)")
            return race, results, payouts
            
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

            # コーナー通過順位とラップタイムを抽出
            corner_lap_data = self._extract_corner_and_lap_data(soup)
            
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
                race_conditions=additional_info.get('race_conditions'),
                corner_positions=corner_lap_data['corner_positions'],
                lap_data=corner_lap_data['lap_data']
            )

            # logger.info(f"RACE: {race}")
            
            return race
            
        except Exception as e:
            logger.error(f"Error extracting race info for {race_id}: {str(e)}")
            return None
    
    def _extract_corner_and_lap_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """コーナー通過順位とラップタイムデータを抽出"""
        data = {
            'corner_positions': None,
            'lap_data': None
        }
        
        try:
            # result_table_02クラスのテーブルを全て取得
            result_tables = soup.find_all('table', class_='result_table_02')
            
            for table in result_tables:
                caption = table.find('caption')
                if not caption:
                    continue
                    
                caption_text = caption.text.strip()
                
                if caption_text == 'コーナー通過順位':
                    data['corner_positions'] = self._extract_corner_positions(table)
                elif caption_text == 'ラップタイム':
                    data['lap_data'] = self._extract_lap_data(table)
            
            logger.debug(f"Corner/Lap data extraction: corners={data['corner_positions'] is not None}, "
                        f"laps={data['lap_data'] is not None}")
            
            return data
            
        except Exception as e:
            logger.warning(f"Error extracting corner/lap data: {str(e)}")
            return data

    def _extract_corner_positions(self, table) -> Optional[Dict[str, str]]:
        """コーナー通過順位テーブルからデータを抽出"""
        try:
            corner_data = {}
            rows = table.find_all('tr')
            
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                
                if th and td:
                    corner_name = th.text.strip()
                    position_data = td.text.strip()
                    
                    # コーナー名を正規化
                    if corner_name == '1コーナー':
                        corner_data['corner_1'] = position_data
                    elif corner_name == '2コーナー':
                        corner_data['corner_2'] = position_data
                    elif corner_name == '3コーナー':
                        corner_data['corner_3'] = position_data
                    elif corner_name == '4コーナー':
                        corner_data['corner_4'] = position_data
            
            logger.debug(f"Extracted corner positions: {list(corner_data.keys())}")
            return corner_data if corner_data else None
            
        except Exception as e:
            logger.warning(f"Error extracting corner positions: {str(e)}")
            return None

    def _extract_lap_data(self, table) -> Optional[Dict[str, str]]:
        """ラップタイムテーブルからデータを抽出"""
        try:
            lap_data = {}
            rows = table.find_all('tr')
            
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                
                if th and td:
                    data_type = th.text.strip()
                    time_data = td.text.strip()
                    
                    # データタイプを正規化
                    if data_type == 'ラップ':
                        lap_data['lap_times'] = time_data
                    elif data_type == 'ペース':
                        lap_data['pace_times'] = time_data
            
            logger.debug(f"Extracted lap data: {list(lap_data.keys())}")
            return lap_data if lap_data else None
            
        except Exception as e:
            logger.warning(f"Error extracting lap data: {str(e)}")
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
        elif '(JpnI)' in race_name or '(Jpn1)' in race_name:
            return 'Jpn1'
        elif '(JpnII)' in race_name or '(Jpn2)' in race_name:
            return 'Jpn2'
        elif '(JpnIII)' in race_name or '(Jpn3)' in race_name:
            return 'Jpn3'
        return None
    
    def _extract_race_conditions(self, racedata_elem) -> Dict[str, Any]:
        """レース条件を抽出（距離、コース、天候、馬場状態、発走時刻）"""
        conditions = {}
        
        try:
            condition_text = racedata_elem.get_text()
            
            # 距離とコース種別 - より柔軟なパターンマッチング
            # 右から左に向かって優先順位でマッチング
            distance_match = re.search(r'(.*?)([右左直].*?)(\d+m)', condition_text)
            if distance_match:
                track_type_raw = distance_match.group(1).strip()
                track_direction = distance_match.group(2).strip()
                distance_str = distance_match.group(3)
                
                # track_typeの正規化
                if track_type_raw.startswith('ダ'):
                    track_type = 'ダート'
                elif track_type_raw.startswith('芝'):
                    track_type = '芝'
                else:
                    track_type = track_type_raw
                
                conditions['track_type'] = track_type
                conditions['track_direction'] = track_direction if track_direction else None
                conditions['distance'] = int(distance_str.rstrip('m'))
            
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
                track_match = re.search(r'回([^日]+?)\d+日目', date_text)
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
    
    def _extract_race_payouts(self, soup: BeautifulSoup, race_id: str) -> List[RacePayout]:
        """払い戻し情報を抽出"""
        payouts = []
        
        try:
            # 払い戻しブロックを取得
            pay_block = soup.find('dl', class_='pay_block')
            if not pay_block:
                logger.warning(f"Payout block not found for {race_id}")
                return payouts
            
            # 払い戻しテーブルを取得（複数のテーブルがある場合がある）
            pay_tables = pay_block.find_all('table', class_='pay_table_01')
            
            for table in pay_tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    try:
                        payout_data = self._extract_payout_row(row, race_id)
                        if payout_data:
                            payouts.extend(payout_data)  # 複勝などは複数の結果がある
                    except Exception as e:
                        logger.warning(f"Error extracting payout row for {race_id}: {str(e)}")
                        continue
            
            logger.info(f"Extracted {len(payouts)} payout records for {race_id}")
            return payouts
            
        except Exception as e:
            logger.error(f"Error extracting payouts for {race_id}: {str(e)}")
            return payouts

    def _extract_payout_row(self, row, race_id: str) -> List[RacePayout]:
        """単一の払い戻し行から情報を抽出（popularity対応版）"""
        payouts = []
        
        try:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 4:
                return payouts
            
            # 券種を取得（thタグのクラスから判定）
            bet_type_elem = cells[0]
            bet_type = self._normalize_bet_type(bet_type_elem.text.strip(), bet_type_elem.get('class', []))
            
            if not bet_type:
                return payouts
            
            # 組み合わせ、配当、人気を取得
            combinations_text = cells[1].get_text(separator='\n', strip=True)
            payouts_text = cells[2].get_text(separator='\n', strip=True)
            popularity_text = cells[3].get_text(separator='\n', strip=True)
            
            # より厳密な分割処理
            combinations = [c.strip() for c in combinations_text.split('\n') if c.strip()]
            payout_amounts = [p.strip() for p in payouts_text.split('\n') if p.strip()]
            popularities = [p.strip() for p in popularity_text.split('\n') if p.strip()]
            
            # 各組み合わせに対してPayoutオブジェクトを作成
            for i in range(len(combinations)):
                try:
                    combination = self._normalize_combination(combinations[i])
                    
                    # 払い戻し金額の解析を改善
                    payout_text = payout_amounts[i].replace(',', '').strip()
                    
                    # 数字以外が含まれている場合はスキップ
                    if not re.match(r'^\d+$', payout_text):
                        logger.warning(f"Invalid payout format: {payout_text}")
                        continue
                    
                    payout_amount = self._parse_decimal(payout_text)
                    
                    # 人気の解析を改善（空白対応）
                    popularity_str = popularities[i] if i < len(popularities) else ""
                    popularity = self._parse_int(popularity_str)  # 空白の場合はNoneが返される
                    
                    # 払い戻し金額の妥当性チェック（上限緩和）
                    if payout_amount is None or payout_amount <= 0:
                        logger.warning(f"Invalid payout amount: {payout_text}")
                        continue
                    
                    # 超高配当のログ出力のみ（除外しない）
                    if payout_amount > Decimal('50000000'):  # 5000万円
                        logger.info(f"Super high payout detected: {payout_amount} ({bet_type} {combination})")
                    
                    if combination and payout_amount is not None:
                        payout = RacePayout(
                            race_id=race_id,
                            bet_type=bet_type,
                            combination=combination,
                            payout_amount=payout_amount,
                            popularity=popularity  # Noneも許可
                        )
                        payouts.append(payout)
                        
                except Exception as e:
                    logger.warning(f"Error creating payout object: {str(e)}")
                    continue
            
            return payouts
            
        except Exception as e:
            logger.warning(f"Error extracting payout row: {str(e)}")
            return payouts


    def _normalize_bet_type(self, bet_text: str, css_classes: List[str]) -> Optional[str]:
        """券種を正規化（枠単対応版）"""
        
        # まずテキストベースで判定（優先）
        bet_text_clean = bet_text.strip()
        text_mapping = {
            '単勝': '単勝',
            '複勝': '複勝', 
            '枠連': '枠連',
            '枠単': '枠単',  # 新規追加
            '馬連': '馬連',
            'ワイド': 'ワイド',
            '馬単': '馬単',
            '三連複': '三連複',
            '三連単': '三連単'
        }
        
        # テキストマッチング（最優先）
        if bet_text_clean in text_mapping:
            return text_mapping[bet_text_clean]
        
        # CSSクラスから判定（フォールバック）
        class_mapping = {
            'tan': '単勝',
            'fuku': '複勝',
            'waku': '枠連',  # デフォルトは枠連（枠単はテキストで判定済み）
            'uren': '馬連',
            'wide': 'ワイド',
            'utan': '馬単',
            'sanfuku': '三連複',
            'santan': '三連単'
        }
        
        for css_class in css_classes:
            if css_class in class_mapping:
                # 特別処理：wakuクラスの場合はテキストも確認
                if css_class == 'waku':
                    if '枠単' in bet_text_clean:
                        return '枠単'
                    else:
                        return '枠連'
                return class_mapping[css_class]
        
        return None

    def _normalize_combination(self, combination_text: str) -> str:
        """組み合わせを正規化"""
        # 全角数字を半角に変換
        combination = combination_text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        
        # スペースを統一
        combination = re.sub(r'\s+', ' ', combination)
        
        # 矢印記号を統一
        combination = combination.replace('→', '→').replace('->', '→')
        
        return combination.strip()
    
    # === パースヘルパーメソッド ===
    
    def _parse_int(self, text: str) -> Optional[int]:
        """安全に整数をパース"""
        try:
            text_stripped = text.strip()
            if not text_stripped or text_stripped == '':
                return None
            return int(text_stripped)
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
        """
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
        """

        try:
            if entity_type == 'horse':
                # 英数字対応
                match = re.search(r'/horse/([a-zA-Z0-9]+)/?', href)
            elif entity_type == 'jockey':
                match = re.search(r'/jockey/result/recent/([a-zA-Z0-9]+)/?', href)
            elif entity_type == 'trainer':
                match = re.search(r'/trainer/result/recent/([a-zA-Z0-9]+)/?', href)
            elif entity_type == 'owner':
                match = re.search(r'/owner/result/recent/([a-zA-Z0-9]+)/?', href)
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