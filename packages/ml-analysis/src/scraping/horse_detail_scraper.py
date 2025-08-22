import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os
from dotenv import load_dotenv
from supabase import create_client, Client

# 環境変数を読み込み
load_dotenv()

class HorseDetailScraper:
    def __init__(self):
        """個別馬詳細情報スクレイピングクラスの初期化"""
        self.base_url = "https://db.netkeiba.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Supabaseクライアント初期化
        supabase_url = os.getenv('NEXT_PUBLIC_SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if supabase_url and supabase_key:
            self.supabase: Client = create_client(supabase_url, supabase_key)
        else:
            print("⚠️ Supabase環境変数が設定されていません")
            self.supabase = None

        # 日本語→英語変換表
        self.field_mapping = {
            # 基本情報
            '馬名': 'name_ja',
            '英字名': 'name_en', 
            '生年月日': 'birth_date',
            '調教師': 'trainer',
            '馬主': 'owner',
            '生産者': 'breeder',
            '産地': 'birthplace',
            
            # 血統情報
            '父': 'sire',
            '母': 'dam', 
            '母父': 'maternal_grandsire',
            
            # 体格情報
            '馬体重': 'weight',
            '体高': 'height',
            
            # 成績情報
            '通算成績': 'career_record',
            '獲得賞金 (中央)': 'total_prize_central',
            '獲得賞金 (地方)': 'total_prize_local',
            '重賞勝利': 'graded_wins',
            
            # 主な勝ち鞍
            '主な勝ち鞍': 'main_victories',
            
            # 募集情報
            '募集情報': 'offering_info',
            
            # その他
            'セリ取引価格': 'auction_price',
            '近親馬': 'related_horses'
        }

        # 関係タイプ定義
        self.relation_types = {
            'sire_of': '父子関係',
            'dam_of': '母子関係', 
            'mating': '種付関係',
            'bms_of': 'BMS関係'
        }

    def scrape_horse_detail(self, horse_id: str) -> Optional[Dict]:
        """個別馬の詳細情報を取得"""
        detail_url = f"{self.base_url}/horse/{horse_id}/"
        
        try:
            print(f"🔍 馬詳細ページ取得: {horse_id}")
            response = self.session.get(detail_url, timeout=15)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 基本情報を抽出
            horse_data = self.extract_basic_info(soup, horse_id)
            
            if horse_data:
                print(f"  ✅ 基本情報取得成功: {horse_data.get('name_ja', 'Unknown')}")
                return horse_data
            else:
                print(f"  ❌ 基本情報取得失敗")
                return None
                
        except Exception as e:
            print(f"  ❌ 詳細取得エラー (馬ID: {horse_id}): {e}")
            return None

    def extract_basic_info(self, soup: BeautifulSoup, horse_id: str) -> Optional[Dict]:
        """基本情報を抽出"""
        try:
            horse_data = {'id': int(horse_id)}
            
            # デバッグ: ページタイトル確認
            title = soup.find('title')
            print(f"    📄 ページタイトル: {title.text if title else 'タイトル未発見'}")
            
            # 馬名を取得
            horse_name = self.extract_horse_name(soup)
            if horse_name:
                horse_data['name_ja'] = horse_name
                print(f"    📝 馬名取得: {horse_data['name_ja']}")
            
            # 性別情報を取得
            sex = self.extract_sex_info(soup)
            if sex:
                horse_data['sex'] = sex
                print(f"    🔤 性別取得: {horse_data['sex']}")
            
            # 英語名を取得
            eng_name = self.extract_english_name(soup)
            if eng_name:
                horse_data['name_en'] = eng_name
                print(f"    🔤 英語名取得: {horse_data['name_en']}")
            
            # プロフィールテーブルを解析
            profile_data = self.extract_profile_data(soup)
            horse_data.update(profile_data)
            
            # 血統IDを直接取得
            pedigree_ids = self.extract_pedigree_ids(soup)
            horse_data.update(pedigree_ids)
            
            # 主な勝ち鞍を抽出
            victories = self.extract_main_victories(soup)
            if victories:
                horse_data['main_victories'] = victories
                print(f"    🏆 勝ち鞍取得: {len(victories)}レース")
            
            # 通算成績を抽出
            career_record = self.extract_career_record(soup)
            if career_record:
                horse_data['career_record'] = career_record
                print(f"    📊 成績取得: {career_record}")
            
            print(f"    📋 取得データ項目数: {len(horse_data)}")
            
            return horse_data if len(horse_data) > 1 else None
            
        except Exception as e:
            print(f"    ❌ 基本情報抽出エラー: {e}")
            return None

    def extract_horse_name(self, soup: BeautifulSoup) -> Optional[str]:
        """馬名を取得"""
        name_patterns = [
            soup.find('div', class_='horse_title'),
            soup.find('h1'),
            soup.find('div', class_='horse_name'),
        ]
        
        for pattern in name_patterns:
            if pattern:
                # h1タグまたはテキストから馬名を取得
                h1_tag = pattern.find('h1') if pattern.name != 'h1' else pattern
                if h1_tag:
                    name_text = h1_tag.get_text(strip=True)
                    if name_text:
                        return name_text
        
        return None

    def extract_sex_info(self, soup: BeautifulSoup) -> Optional[str]:
        """性別情報を取得（日本語→英語変換）"""
        sex_info = soup.find('div', class_='horse_title')
        if sex_info:
            sex_text = sex_info.find('p', class_='txt_01')
            if sex_text:
                text = sex_text.get_text(strip=True)
                if '牡' in text:
                    return 'stallion'
                elif '牝' in text:
                    return 'mare'
                elif 'せん' in text:
                    return 'gelding'
        
        return None

    def extract_english_name(self, soup: BeautifulSoup) -> Optional[str]:
        """英語名を取得"""
        eng_name = soup.find('p', class_='eng_name')
        if eng_name:
            eng_link = eng_name.find('a')
            if eng_link:
                return eng_link.get_text(strip=True)
        
        return None

    def extract_profile_data(self, soup: BeautifulSoup) -> Dict:
        """プロフィールテーブルからデータを取得"""
        profile_data = {}
        
        # プロフィールテーブルを探す
        profile_table = self.find_profile_table(soup)
        
        if profile_table:
            print(f"    ✅ プロフィールテーブル発見")
            profile_data = self.parse_profile_table(profile_table)
        else:
            print(f"    ❌ プロフィールテーブル未発見")
            # デバッグ情報を表示
            self.debug_tables(soup)
        
        return profile_data

    def find_profile_table(self, soup: BeautifulSoup):
        """プロフィールテーブルを探す"""
        table_patterns = [
            ('summary', 'のプロフィール'),
            ('summary', 'プロフィール'),
            ('class', 'db_prof_table'),
            ('class', 'horse_info'),
            ('class', 'prof_table'),
            ('class', 'horse_prof')
        ]
        
        for attr, value in table_patterns:
            if attr == 'summary':
                table = soup.find('table', summary=value)
            elif attr == 'class':
                table = soup.find('table', class_=value)
            
            if table:
                print(f"    ✅ プロフィールテーブル発見: {attr}='{value}'")
                return table
        
        return None

    def debug_tables(self, soup: BeautifulSoup):
        """デバッグ用：ページ内の全テーブル情報を表示"""
        all_tables = soup.find_all('table')
        print(f"    🔍 ページ内の全テーブル数: {len(all_tables)}")
        
        for i, table in enumerate(all_tables[:5]):  # 最初の5個だけ
            summary = table.get('summary', 'なし')
            class_name = table.get('class', 'なし')
            print(f"      テーブル{i+1}: summary='{summary}', class='{class_name}'")

    def parse_profile_table(self, table) -> Dict:
        """プロフィールテーブルを解析"""
        profile_data = {}
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                # 日本語ラベル
                label = cells[0].get_text(strip=True)
                value_cell = cells[1]
                
                # 英語フィールド名に変換
                field_name = self.field_mapping.get(label)
                if field_name:
                    value = self.extract_field_value(value_cell, field_name)
                    if value:
                        profile_data[field_name] = value
                        print(f"      - {label} ({field_name}): {value}")
        
        return profile_data

    def extract_pedigree_ids(self, soup: BeautifulSoup) -> Dict:
        """血統テーブルから直接カラム用のIDを抽出"""
        try:
            pedigree_ids = {}
            
            # 血統テーブルを探す
            blood_table = soup.find('table', class_='blood_table')
            if not blood_table:
                return pedigree_ids
            
            rows = blood_table.find_all('tr')
            if len(rows) >= 4:
                # 1行目: 父
                sire_link = rows[0].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if sire_link:
                    href = sire_link.get('href', '')
                    sire_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if sire_id_match:
                        pedigree_ids['sire_id'] = int(sire_id_match.group(1))
                        print(f"    🧬 父ID取得: {pedigree_ids['sire_id']}")
                
                # 3行目: 母
                dam_link = rows[2].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if dam_link:
                    href = dam_link.get('href', '')
                    dam_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if dam_id_match:
                        pedigree_ids['dam_id'] = int(dam_id_match.group(1))
                        print(f"    🧬 母ID取得: {pedigree_ids['dam_id']}")
                
                # 4行目: 母父
                bms_link = rows[3].find('a', href=re.compile(r'/horse/ped/[0-9a-zA-Z]+/'))
                if bms_link:
                    href = bms_link.get('href', '')
                    bms_id_match = re.search(r'/horse/ped/([0-9a-zA-Z]+)/', href)
                    if bms_id_match:
                        pedigree_ids['maternal_grandsire_id'] = int(bms_id_match.group(1))
                        print(f"    🧬 母父ID取得: {pedigree_ids['maternal_grandsire_id']}")
            
            return pedigree_ids
            
        except Exception as e:
            print(f"      ❌ 血統ID抽出エラー: {e}")
            return {}

    def extract_field_value(self, cell, field_name: str):
        """フィールドタイプに応じて値を抽出"""
        try:
            if field_name in ['sire', 'dam', 'maternal_grandsire']:
                # 血統情報：リンクからIDと名前を抽出
                link = cell.find('a', href=re.compile(r'/horse/[0-9a-zA-Z]+/'))
                if link:
                    href = link.get('href', '')
                    id_match = re.search(r'/horse/([0-9a-zA-Z]+)/', href)
                    if id_match:
                        return {
                            'id': id_match.group(1),
                            'name': link.get_text(strip=True)
                        }
            
            elif field_name == 'birth_date':
                # 生年月日：YYYY年M月D日 → YYYY-MM-DD
                text = cell.get_text(strip=True)
                date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text)
                if date_match:
                    year, month, day = date_match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            elif field_name in ['total_prize_central', 'total_prize_local']:
                # 獲得賞金：数値部分のみ抽出（万円単位）
                text = cell.get_text(strip=True)
                # "17億5,655万円" や "0万円" を処理
                if '億' in text and '万円' in text:
                    # 億と万円両方ある場合
                    oku_match = re.search(r'(\d+)億', text)
                    man_match = re.search(r'(\d{1,4})万円', text)
                    if oku_match and man_match:
                        oku = int(oku_match.group(1)) * 10000  # 億を万円に変換
                        man = int(man_match.group(1).replace(',', ''))
                        return oku + man
                elif '万円' in text:
                    # 万円のみ
                    prize_match = re.search(r'([\d,]+)万円', text)
                    if prize_match:
                        return int(prize_match.group(1).replace(',', ''))
                return 0
            
            elif field_name == 'career_record':
                # 通算成績：10戦8勝 [8-2-0-0] を解析
                text = cell.get_text(strip=True)
                record_match = re.search(r'(\d+)戦(\d+)勝', text)
                detail_match = re.search(r'\[(\d+)-(\d+)-(\d+)-(\d+)\]', text)
                
                if record_match:
                    starts = int(record_match.group(1))
                    wins = int(record_match.group(2))
                    
                    result = {
                        'starts': starts,
                        'wins': wins,
                        'win_rate': round(wins / starts * 100, 1) if starts > 0 else 0
                    }
                    
                    if detail_match:
                        result.update({
                            'first': int(detail_match.group(1)),
                            'second': int(detail_match.group(2)), 
                            'third': int(detail_match.group(3)),
                            'others': int(detail_match.group(4))
                        })
                    
                    return result
            
            elif field_name == 'offering_info':
                # 募集情報：1口:8万円/500口 を解析
                text = cell.get_text(strip=True)
                offering_match = re.search(r'1口:(\d+)万円/(\d+)口', text)
                if offering_match:
                    return {
                        'price_per_unit': int(offering_match.group(1)),
                        'total_units': int(offering_match.group(2)),
                        'raw_text': text
                    }
                elif text and text != '-':
                    return {'raw_text': text}
            
            else:
                # その他：テキストをそのまま
                text = cell.get_text(strip=True)
                return text if text and text != '-' else None
                
        except Exception as e:
            print(f"      ⚠️ フィールド値抽出エラー ({field_name}): {e}")
            return None

    def extract_main_victories(self, soup: BeautifulSoup) -> Optional[List[Dict]]:
        """主な勝ち鞍を抽出"""
        try:
            victories = []
            
            # 主な勝ち鞍セクションを探す
            victory_section = soup.find('div', class_='horse_result')
            if not victory_section:
                return None
            
            # G1, G2, G3の勝利を抽出
            race_links = victory_section.find_all('a', href=re.compile(r'/race/\d+/'))
            
            for link in race_links:
                race_text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # グレード判定
                grade = None
                if 'G1' in race_text or '(G1)' in race_text:
                    grade = 'G1'
                elif 'G2' in race_text or '(G2)' in race_text:
                    grade = 'G2'
                elif 'G3' in race_text or '(G3)' in race_text:
                    grade = 'G3'
                
                if grade:
                    race_id = re.search(r'/race/(\d+)/', href)
                    victories.append({
                        'race_name': race_text,
                        'race_id': race_id.group(1) if race_id else None,
                        'grade': grade
                    })
            
            return victories if victories else None
            
        except Exception as e:
            print(f"    ⚠️ 勝ち鞍抽出エラー: {e}")
            return None

    def extract_career_record(self, soup: BeautifulSoup) -> Optional[Dict]:
        """通算成績を抽出"""
        try:
            # 成績テーブルを探す
            record_element = soup.find('table', summary='競走成績')
            if not record_element:
                return None
            
            # "XX戦X勝"形式をパース
            text = record_element.get_text()
            record_match = re.search(r'(\d+)戦(\d+)勝', text)
            
            if record_match:
                starts = int(record_match.group(1))
                wins = int(record_match.group(2))
                
                return {
                    'starts': starts,
                    'wins': wins,
                    'win_rate': round(wins / starts * 100, 1) if starts > 0 else 0
                }
                
        except Exception as e:
            print(f"    ⚠️ 成績抽出エラー: {e}")
            
        return None

    def scrape_pedigree_relations(self, horse_id: str) -> List[Dict]:
        """血統関係を取得"""
        pedigree_url = f"{self.base_url}/horse/ped/{horse_id}/"
        relations = []
        
        try:
            print(f"  🌳 血統関係取得: {horse_id}")
            response = self.session.get(pedigree_url, timeout=15)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 血統表から父・母・母父を抽出
            pedigree_table = self.find_pedigree_table(soup)
            
            if pedigree_table:
                relations.extend(self.extract_direct_relations(pedigree_table, horse_id))
            
            return relations
            
        except Exception as e:
            print(f"    ❌ 血統関係取得エラー: {e}")
            return []

    def find_pedigree_table(self, soup: BeautifulSoup):
        """血統表を探す"""
        table_candidates = [
            soup.find('table', class_='blood_table_detail'),
            soup.find('table', summary='5代血統表'),
            soup.find('table', class_='blood_table')
        ]
        
        for table in table_candidates:
            if table:
                return table
        
        return None

    def extract_direct_relations(self, table, horse_id: str) -> List[Dict]:
        """直接的な血統関係（父・母・母父）を抽出"""
        relations = []  # この行を追加
        
        try:
            rows = table.find_all('tr')
            
            for row_index, row in enumerate(rows):
                cells = row.find_all('td')
                
                for cell in cells:
                    horse_link = cell.find('a', href=re.compile(r'/horse/[0-9a-zA-Z]+/'))
                    
                    if horse_link:
                        href = horse_link.get('href', '')
                        horse_id_match = re.search(r'/horse/([0-9a-zA-Z]+)/', href)
                        
                        if horse_id_match:
                            related_horse_id = horse_id_match.group(1)
                            horse_name = horse_link.get_text(strip=True)
                            
                            # 位置から関係タイプを判定
                            rowspan = cell.get('rowspan', '1')
                            relation_type = self.determine_relation_type(row_index, rowspan)
                            
                            if relation_type:
                                relations.append({
                                    'horse_a_id': int(related_horse_id),
                                    'horse_b_id': int(horse_id),
                                    'relation_type': relation_type,
                                    'children_ids': None  # 単純な親子関係
                                })
            
        except Exception as e:
            print(f"      ❌ 関係抽出エラー: {e}")
        
        return relations

    def extract_pedigree_ids_from_relations(self, relations: List[Dict], horse_id: str) -> Dict:
        """血統関係から直接カラム用のIDを抽出"""
        pedigree_ids = {}
        
        for relation in relations:
            if relation['horse_b_id'] == int(horse_id):
                if relation['relation_type'] == 'sire_of':
                    pedigree_ids['sire_id'] = relation['horse_a_id']
                    print(f"    🧬 父ID設定: {pedigree_ids['sire_id']}")
                elif relation['relation_type'] == 'dam_of':
                    pedigree_ids['dam_id'] = relation['horse_a_id']
                    print(f"    🧬 母ID設定: {pedigree_ids['dam_id']}")
                elif relation['relation_type'] == 'bms_of':
                    pedigree_ids['maternal_grandsire_id'] = relation['horse_a_id']
                    print(f"    🧬 母父ID設定: {pedigree_ids['maternal_grandsire_id']}")
        
        return pedigree_ids

    def create_mating_relations(self, pedigree_ids: Dict) -> List[Dict]:
        """父と母の種付関係を作成"""
        mating_relations = []
        
        sire_id = pedigree_ids.get('sire_id')
        dam_id = pedigree_ids.get('dam_id')
        
        if sire_id and dam_id:
            mating_relation = {
                'horse_a_id': sire_id,  # 父
                'horse_b_id': dam_id,   # 母
                'relation_type': 'mating',
                'children_ids': None  # 今回は使わない
            }
            mating_relations.append(mating_relation)
            print(f"    💕 種付関係追加: {sire_id} × {dam_id}")
        
        return mating_relations

    def determine_relation_type(self, row_index: int, rowspan: str) -> Optional[str]:
        """血統表の位置から関係タイプを判定"""
        try:
            # rowspanから世代を判定
            if rowspan == '16':  # 1世代目
                if row_index == 0:
                    return 'sire_of'  # 父
                elif row_index >= 16:
                    return 'dam_of'   # 母
            elif rowspan == '8':  # 2世代目
                if row_index == 16:  # 母父の位置
                    return 'bms_of'
            
        except:
            pass
        
        return None
        """血統関係から直接カラム用のIDを抽出"""
        pedigree_ids = {}
        
        for relation in relations:
            if relation['horse_b_id'] == int(horse_id):
                if relation['relation_type'] == 'sire_of':
                    pedigree_ids['sire_id'] = relation['horse_a_id']
                    print(f"    🧬 父ID設定: {pedigree_ids['sire_id']}")
                elif relation['relation_type'] == 'dam_of':
                    pedigree_ids['dam_id'] = relation['horse_a_id']
                    print(f"    🧬 母ID設定: {pedigree_ids['dam_id']}")
                elif relation['relation_type'] == 'bms_of':
                    pedigree_ids['maternal_grandsire_id'] = relation['horse_a_id']
                    print(f"    🧬 母父ID設定: {pedigree_ids['maternal_grandsire_id']}")
        
        return pedigree_ids

    def create_mating_relations(self, pedigree_ids: Dict) -> List[Dict]:
        """父と母の種付関係を作成"""
        mating_relations = []
        
        sire_id = pedigree_ids.get('sire_id')
        dam_id = pedigree_ids.get('dam_id')
        
        if sire_id and dam_id:
            mating_relation = {
                'horse_a_id': sire_id,  # 父
                'horse_b_id': dam_id,   # 母
                'relation_type': 'mating',
                'children_ids': None  # 今回は使わない
            }
            mating_relations.append(mating_relation)
            print(f"    💕 種付関係追加: {sire_id} × {dam_id}")
        
        return mating_relations
        """血統表の位置から関係タイプを判定"""
        try:
            # rowspanから世代を判定
            if rowspan == '16':  # 1世代目
                if row_index == 0:
                    return 'sire_of'  # 父
                elif row_index >= 16:
                    return 'dam_of'   # 母
            elif rowspan == '8':  # 2世代目
                if row_index == 16:  # 母父の位置
                    return 'bms_of'
            
        except:
            pass
        
        return None

    def save_horse_data(self, horse_data: Dict) -> bool:
        """馬の基本情報をSupabaseに保存"""
        if not self.supabase:
            return False
        
        try:
            # 直接カラム用のデータを分離
            direct_columns = {
                'id': horse_data['id'],
                'name_ja': horse_data.get('name_ja'),
                'name_en': horse_data.get('name_en'),
                'birth_date': horse_data.get('birth_date'),
                'sex': horse_data.get('sex'),
                'sire_id': horse_data.get('sire_id'),
                'dam_id': horse_data.get('dam_id'),
                'maternal_grandsire_id': horse_data.get('maternal_grandsire_id')
            }
            
            # profile用のデータ（直接カラム以外）
            profile_data = {
                key: value for key, value in horse_data.items() 
                if key not in ['id', 'name_ja', 'name_en', 'birth_date', 'sex', 'sire_id', 'dam_id', 'maternal_grandsire_id']
            }
            
            # 最終的な保存データ
            save_data = {**direct_columns, 'profile': profile_data}
            
            # None値を除去
            save_data = {k: v for k, v in save_data.items() if v is not None}
            
            # upsertで保存
            result = self.supabase.table('horses').upsert(
                save_data, on_conflict='id'
            ).execute()
            
            print(f"    💾 保存データ構造:")
            print(f"      直接カラム: {list(direct_columns.keys())}")
            print(f"      プロフィール項目: {list(profile_data.keys())}")
            
            return True
            
        except Exception as e:
            print(f"    ❌ 馬データ保存エラー: {e}")
            return False

    def save_relations(self, relations: List[Dict]) -> bool:
        """血統関係をSupabaseに保存"""
        if not self.supabase or not relations:
            return False
        
        try:
            saved_count = 0
            
            for relation in relations:
                # 重複チェック
                existing = self.supabase.table('horse_relations').select('id').eq(
                    'horse_a_id', relation['horse_a_id']
                ).eq('horse_b_id', relation['horse_b_id']).eq(
                    'relation_type', relation['relation_type']
                ).execute()
                
                if not existing.data:
                    clean_relation = {
                        'horse_a_id': relation['horse_a_id'],
                        'horse_b_id': relation['horse_b_id'],
                        'relation_type': relation['relation_type'],
                        'children_ids': relation.get('children_ids')
                    }
                    
                    self.supabase.table('horse_relations').insert(clean_relation).execute()
                    saved_count += 1
                    print(f"      💾 関係保存: {relation['relation_type']} ({relation['horse_a_id']} -> {relation['horse_b_id']})")
                else:
                    print(f"      💡 関係既存: {relation['relation_type']}")
            
            print(f"    ✅ {saved_count}件の関係を保存")
            return True
            
        except Exception as e:
            print(f"    ❌ 関係保存エラー: {e}")
            return False

    def scrape_horse_complete(self, horse_id: str) -> bool:
        """馬の完全な情報を取得・保存"""
        print(f"\n--- 馬ID: {horse_id} 完全取得開始 ---")
        
        # 1. 基本情報取得
        horse_data = self.scrape_horse_detail(horse_id)
        if not horse_data:
            return False
        
        # 2. 血統関係取得
        relations = self.scrape_pedigree_relations(horse_id)
        
        # 3. 血統IDを馬データに追加
        pedigree_ids = self.extract_pedigree_ids_from_relations(relations, horse_id)
        horse_data.update(pedigree_ids)
        
        # 4. 種付関係を追加
        mating_relations = self.create_mating_relations(pedigree_ids)
        relations.extend(mating_relations)
        
        # 5. データ保存
        horse_saved = self.save_horse_data(horse_data)
        relations_saved = self.save_relations(relations) if relations else True
        
        success = horse_saved and relations_saved
        print(f"{'✅' if success else '❌'} 完了: {horse_data.get('name_ja', 'Unknown')}")
        
        # リクエスト間隔を置く
        time.sleep(1)
        
        return success

# 使用例
if __name__ == "__main__":
    scraper = HorseDetailScraper()
    
    # テスト実行（イクイノックス）
    test_horse_id = "2019105219"
    success = scraper.scrape_horse_complete(test_horse_id)
    
    if success:
        print("🎉 詳細情報取得完了！")
    else:
        print("❌ 取得失敗")