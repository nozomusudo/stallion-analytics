# src/scraping/extractors/horse/basic_info_extractor.py

import sys
import os
from typing import Dict, Optional
from bs4 import BeautifulSoup

# パスを追加
current_dir = os.path.dirname(os.path.abspath(__file__))
scraping_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(scraping_dir)

from utils.constants import HORSE_FIELD_MAPPING, SEX_MAPPING
from parsers.field_parser import FieldParser

class BasicInfoExtractor:
    """馬の基本情報を抽出するクラス"""
    
    def __init__(self):
        self.field_mapping = HORSE_FIELD_MAPPING
        self.parser = FieldParser()
    
    def extract(self, soup: BeautifulSoup, horse_id: str) -> Dict:
        """基本情報を抽出"""
        horse_data = {'id': horse_id}
        
        # ページタイトルチェック
        title = soup.find('title')
        print(f"    📄 ページタイトル: {title.text if title else 'タイトル未発見'}")
        
        # 各情報を抽出
        horse_data.update(self.extract_horse_name(soup))
        horse_data.update(self.extract_sex_info(soup))
        horse_data.update(self.extract_english_name(soup))
        horse_data.update(self.extract_profile_data(soup))
        
        return horse_data
    
    def extract_horse_name(self, soup: BeautifulSoup) -> Dict:
        """馬名を取得"""
        name_patterns = [
            soup.find('div', class_='horse_title'),
            soup.find('h1'),
            soup.find('div', class_='horse_name'),
        ]
        
        for pattern in name_patterns:
            if pattern:
                h1_tag = pattern.find('h1') if pattern.name != 'h1' else pattern
                if h1_tag:
                    name_text = h1_tag.get_text(strip=True)
                    if name_text:
                        print(f"    📝 馬名取得: {name_text}")
                        return {'name_ja': name_text}
        
        return {}
    
    def extract_sex_info(self, soup: BeautifulSoup) -> Dict:
        """性別情報を取得"""
        sex_info = soup.find('div', class_='horse_title')
        if sex_info:
            sex_text = sex_info.find('p', class_='txt_01')
            if sex_text:
                text = sex_text.get_text(strip=True)
                for jp_sex, en_sex in SEX_MAPPING.items():
                    if jp_sex in text:
                        print(f"    🔤 性別取得: {en_sex}")
                        return {'sex': en_sex}
        
        return {}
    
    def extract_english_name(self, soup: BeautifulSoup) -> Dict:
        """英語名を取得"""
        eng_name = soup.find('p', class_='eng_name')
        if eng_name:
            eng_link = eng_name.find('a')
            if eng_link:
                eng_name_text = eng_link.get_text(strip=True)
                print(f"    🔤 英語名取得: {eng_name_text}")
                return {'name_en': eng_name_text}
        
        return {}
    
    def extract_profile_data(self, soup: BeautifulSoup) -> Dict:
        """プロフィールテーブルからデータを取得"""
        profile_table = self.find_profile_table(soup)
        
        if profile_table:
            print(f"    ✅ プロフィールテーブル発見")
            return self.parse_profile_table(profile_table)
        else:
            print(f"    ❌ プロフィールテーブル未発見")
            return {}
    
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
                return table
        
        return None
    
    def parse_profile_table(self, table) -> Dict:
        """プロフィールテーブルを解析"""
        profile_data = {}
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value_cell = cells[1]
                
                field_name = self.field_mapping.get(label)
                if field_name:
                    value = self.extract_field_value(value_cell, field_name)
                    if value:
                        profile_data[field_name] = value
                        print(f"      - {label} ({field_name}): {value}")
        
        return profile_data
    
    def extract_field_value(self, cell, field_name: str):
        """フィールドタイプに応じて値を抽出"""
        try:
            if field_name in ['sire', 'dam', 'maternal_grandsire']:
                return self.parser.parse_horse_link(cell)
            elif field_name == 'birth_date':
                text = cell.get_text(strip=True)
                return self.parser.parse_date(text)
            elif field_name in ['total_prize_central', 'total_prize_local']:
                text = cell.get_text(strip=True)
                return self.parser.parse_prize(text)
            elif field_name == 'career_record':
                text = cell.get_text(strip=True)
                return self.parser.parse_career_record(text)
            elif field_name == 'offering_info':
                text = cell.get_text(strip=True)
                return self.parser.parse_offering_info(text)
            else:
                return self.parser.clean_text(cell)
        
        except Exception as e:
            print(f"      ⚠️ フィールド値抽出エラー ({field_name}): {e}")
            return None