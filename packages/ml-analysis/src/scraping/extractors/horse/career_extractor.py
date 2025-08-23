# src/scraping/extractors/horse/career_extractor.py

import sys
import os
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

# パスを追加
current_dir = os.path.dirname(os.path.abspath(__file__))
scraping_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(scraping_dir)

from utils.constants import RACE_GRADES

class CareerExtractor:
    """競走成績・勝鞍情報を抽出するクラス"""
    
    def extract_main_victories(self, soup: BeautifulSoup) -> Optional[List[Dict]]:
        """主な勝ち鞍を抽出"""
        try:
            victories = []
            
            victory_section = soup.find('div', class_='horse_result')
            if not victory_section:
                return None
            
            race_links = victory_section.find_all('a', href=re.compile(r'/race/\d+/'))
            
            for link in race_links:
                race_text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # グレード判定
                grade = self.determine_race_grade(race_text)
                
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
    
    def determine_race_grade(self, race_text: str) -> Optional[str]:
        """レースグレードを判定"""
        for grade in RACE_GRADES:
            if f'{grade}' in race_text or f'({grade})' in race_text:
                return grade
        return None
    
    def extract_career_record(self, soup: BeautifulSoup) -> Optional[Dict]:
        """通算成績を抽出"""
        try:
            record_element = soup.find('table', summary='競走成績')
            if not record_element:
                return None
            
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