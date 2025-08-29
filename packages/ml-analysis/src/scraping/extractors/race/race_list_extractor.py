"""
レース一覧ページからのデータ抽出
src/scraping/extractors/race/race_list_extractor.py
"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class RaceListExtractor:
    """レース一覧ページからレース基本情報を抽出するクラス"""
    
    def __init__(self):
        self.base_url = "https://db.netkeiba.com"

    
    
    def extract_race_list(self, html: str) -> List[Dict[str, Any]]:
        """
        レース一覧ページのHTMLからレース情報を抽出
        
        Args:
            html: レース一覧ページのHTML
            
        Returns:
            List[Dict]: レース基本情報のリスト
        """
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            races = []
            
            # レース一覧テーブルを探す
            # 提供されたHTMLサンプルの構造に基づく
            race_table = soup.find('table', summary='レース検索結果')
        
            if not race_table:
                logger.warning("Race result table not found")
                # デバッグ用に他のテーブルを確認
                all_tables = soup.find_all('table')
                logger.info(f"Found {len(all_tables)} tables total")
                for i, table in enumerate(all_tables):
                    summary = table.get('summary', '')
                    class_name = table.get('class', [])
                    logger.info(f"Table {i}: summary='{summary}', class={class_name}")
                return races

            # logger.info(f"race-tables: {race_table}")
            
            # tbody内の行を取得（最初の行はヘッダーなのでスキップ）
            all_rows = race_table.find_all('tr')
            # logger.info(f"ALL ROWS: {all_rows}")
            race_rows = all_rows[1:]  # ヘッダー行をスキップ
            
            if not race_rows:
                logger.warning("No race rows found in the HTML")
                return races
            
            for row in race_rows:
                try:
                    # logger.info(f"Processing row: {row}")
                    race_data = self._extract_race_row(row)
                    if race_data:
                        races.append(race_data)
                except Exception as e:
                    logger.warning(f"Error extracting race row: {str(e)}")
                    continue
            
            logger.info(f"Extracted {len(races)} races from list")
            return races
            
        except Exception as e:
            logger.error(f"Error parsing race list HTML: {str(e)}")
            return []
    
    def _extract_race_row(self, row) -> Optional[Dict[str, Any]]:
        """
        単一のレース行からデータを抽出
        
        Args:
            row: BeautifulSoupのtr要素
            
        Returns:
            Dict: レース基本情報
        """
        
        try:
            cells = row.find_all('td')
            
            if len(cells) < 10:  # 最低限必要なセル数
                return None
            
            race_data = {}
            
            # 開催日 (1列目)
            date_cell = cells[0]
            date_link = date_cell.find('a')
            if date_link:
                date_text = date_link.text.strip()
                race_data['race_date'] = self._parse_date(date_text)
            
            # 開催場所 (2列目) - "2東京10" のような形式
            venue_cell = cells[1]
            venue_link = venue_cell.find('a')
            if venue_link:
                venue_text = venue_link.text.strip()
                race_data.update(self._parse_venue_info(venue_text))
            
            # 天気 (3列目)
            weather_cell = cells[2]
            race_data['weather'] = weather_cell.text.strip()
            
            # レース番号 (4列目)
            race_num_cell = cells[3]
            race_data['race_number'] = self._parse_int(race_num_cell.text.strip())
            
            # レース名 (5列目) - リンクからレースIDも取得
            race_name_cell = cells[4]
            race_link = race_name_cell.find('a')
            if race_link:
                race_data['race_name'] = race_link.text.strip()
                race_data['race_id'] = self._extract_race_id(race_link.get('href', ''))
                race_data['grade'] = self._extract_grade(race_data['race_name'])
            
            logger.info(f"race-name: {race_data['race_name']}")
            
            # 距離 (6列目) - "芝2400"のような形式
            distance_cell = cells[6]
            distance_text = distance_cell.text.strip()
            race_data.update(self._parse_distance_info(distance_text))
            
            # 頭数 (7列目)
            horses_cell = cells[7]
            race_data['total_horses'] = self._parse_int(horses_cell.text.strip())
            
            # 馬場状態 (8列目)
            condition_cell = cells[8]
            race_data['track_condition'] = condition_cell.text.strip()
            
            # タイム (9列目)
            time_cell = cells[9]
            race_data['winning_time'] = time_cell.text.strip()
            
            # ペース (10列目)
            if len(cells) > 10:
                pace_cell = cells[10]
                race_data['pace'] = pace_cell.text.strip()
            
            # 勝ち馬、騎手、調教師 (11-13列目)
            if len(cells) > 13:
                winner_cell = cells[11]
                winner_link = winner_cell.find('a')
                if winner_link:
                    race_data['winner_name'] = winner_link.text.strip()
                
                jockey_cell = cells[12]
                jockey_link = jockey_cell.find('a')
                if jockey_link:
                    race_data['winner_jockey'] = jockey_link.text.strip()
                
                trainer_cell = cells[13]
                trainer_link = trainer_cell.find('a')
                if trainer_link:
                    trainer_text = trainer_link.text.strip()
                    race_data['winner_trainer'] = trainer_text
                    # [西]や[東]を抽出
                    region_match = re.search(r'\[(東|西)\]', trainer_cell.text)
                    if region_match:
                        race_data['winner_trainer_region'] = region_match.group(1)
            
            return race_data
            
        except Exception as e:
            logger.warning(f"Error extracting race row data: {str(e)}")
            return None
    
    def _parse_date(self, date_text: str) -> Optional[str]:
        """日付文字列をパース"""
        try:
            # "2025/05/25" -> "2025-05-25"
            if '/' in date_text:
                return date_text.replace('/', '-')
            return date_text
        except Exception:
            return None
    
    def _parse_venue_info(self, venue_text: str) -> Dict[str, Any]:
        """開催情報をパース"""
        info = {}
        
        try:
            # "2東京10" のような形式から情報を抽出
            match = re.match(r'(\d+)([^\d]+)(\d+)', venue_text)
            if match:
                meeting_num = match.group(1)  # 2
                track_name = match.group(2)   # 東京
                day_num = match.group(3)      # 10
                
                info['track_name'] = track_name
                info['meeting_number'] = int(meeting_num)
                info['day_number'] = int(day_num)
            else:
                info['track_name'] = venue_text
                
        except Exception:
            info['track_name'] = venue_text
            
        return info
    
    def _extract_race_id(self, href: str) -> Optional[str]:
        """レースURLからレースIDを抽出"""
        try:
            # "/race/202505021011/" -> "202505021011"
            match = re.search(r'/race/(\d{12})/?', href)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
    def _extract_grade(self, race_name: str) -> Optional[str]:
        """レース名からグレードを抽出"""
        try:
            # "優駿牝馬(GI)" -> "G1"
            if '(GI)' in race_name or '(G1)' in race_name:
                return 'G1'
            elif '(GII)' in race_name or '(G2)' in race_name:
                return 'G2'
            elif '(GIII)' in race_name or '(G3)' in race_name:
                return 'G3'
            elif '(L)' in race_name or 'Listed' in race_name:
                return 'Listed'
            elif 'OP' in race_name or 'オープン' in race_name:
                return 'OP'
            elif '1600万' in race_name:
                return '1600万'
            elif '1000万' in race_name:
                return '1000万'
            elif '500万' in race_name:
                return '500万'
            elif '未勝利' in race_name:
                return '未勝利'
        except Exception:
            pass
        return None
    
    def _parse_distance_info(self, distance_text: str) -> Dict[str, Any]:
        """距離情報をパース"""
        info = {}
        
        try:
            # "芝2400" -> track_type="芝", distance=2400
            # "ダ1200" -> track_type="ダート", distance=1200
            # "芝左2400" -> track_type="芝", track_direction="左", distance=2400
            
            distance_text = distance_text.replace('ダ', 'ダート')
            
            if '芝' in distance_text:
                info['track_type'] = '芝'
                remaining = distance_text.replace('芝', '')
            elif 'ダート' in distance_text:
                info['track_type'] = 'ダート' 
                remaining = distance_text.replace('ダート', '')
            else:
                # 数字のみの場合
                remaining = distance_text
                info['track_type'] = '芝'  # デフォルト
            
            # 方向を抽出
            if '左' in remaining:
                info['track_direction'] = '左'
                remaining = remaining.replace('左', '')
            elif '右' in remaining:
                info['track_direction'] = '右'
                remaining = remaining.replace('右', '')
            elif '直線' in remaining:
                info['track_direction'] = '直線'
                remaining = remaining.replace('直線', '')
            
            # 距離を抽出
            distance_match = re.search(r'(\d+)', remaining)
            if distance_match:
                info['distance'] = int(distance_match.group(1))
            
        except Exception as e:
            logger.warning(f"Error parsing distance info '{distance_text}': {str(e)}")
            
        return info
    
    def _parse_int(self, text: str) -> Optional[int]:
        """整数文字列を安全にパース"""
        try:
            return int(text.strip())
        except (ValueError, AttributeError):
            return None

# 使用例とテスト用コード
if __name__ == "__main__":
    import sys
    import os
    
    # ログ設定
    logging.basicConfig(level=logging.INFO)
    
    # サンプルHTMLでテスト
    sample_html = '''
    <tbody>
    <tr>
    <td class="txt_c" nowrap="nowrap"><a href="/race/list/20250525/">2025/05/25</a></td>
    <td class="txt_c" nowrap="nowrap"><a href="/race/sum/05/20250525/">2東京10</a></td>
    <td class="txt_c" nowrap="nowrap">曇</td>
    <td class="txt_r" nowrap="nowrap">11</td>
    <td class="txt_l w_race" nowrap="nowrap"><a href="/race/202505021011/" title="優駿牝馬(GI)">優駿牝馬(GI)</a></td>
    <td class="txt_c" nowrap="nowrap">
    <a href="/race/movie/202505021011" target="_blank"><img src="/style/netkeiba.ja/image/icon_douga.png" border="0" class="png_img"></a>
    </td>
    <td class="txt_c" nowrap="nowrap">芝2400</td>
    <td class="txt_r" nowrap="nowrap">18</td>
    <td class="txt_c" nowrap="nowrap">良</td>
    <td class="txt_r" nowrap="nowrap">2:25.7</td>
    <td class="txt_c" nowrap="nowrap">34.8-34.7</td>
    <td class="txt_l w_horse" nowrap="nowrap">
    <a href="/horse/2022105402/" title="カムニャック" id="umalink_202505021011">カムニャック</a>
    </td>
    <td class="txt_l w_human" nowrap="nowrap">
    <a href="/jockey/result/recent/05115/" title="シュタル">シュタル</a>
    </td>
    <td class="txt_l w_human" nowrap="nowrap">
    [西]<a href="/trainer/result/recent/01061/" title="友道康夫">友道康夫</a>
    </td>
    </tr>
    </tbody>
    '''
    
    extractor = RaceListExtractor()
    races = extractor.extract_race_list(sample_html)
    
    print("=== Extracted Race Data ===")
    for race in races:
        print(f"Race ID: {race.get('race_id')}")
        print(f"Name: {race.get('race_name')}")
        print(f"Date: {race.get('race_date')}")
        print(f"Track: {race.get('track_name')}")
        print(f"Grade: {race.get('grade')}")
        print(f"Distance: {race.get('distance')}m ({race.get('track_type')})")
        print(f"Horses: {race.get('total_horses')}")
        print(f"Winner: {race.get('winner_name')} ({race.get('winner_jockey')})")
        print("---")