# src/scraping/parsers/field_parser.py

import re
from typing import Dict, Optional, Any
from bs4 import BeautifulSoup

class FieldParser:
    """フィールド値のパース処理を行うクラス"""
    
    @staticmethod
    def parse_horse_link(cell) -> Optional[Dict]:
        """血統情報のリンクからIDと名前を抽出"""
        link = cell.find('a', href=re.compile(r'/horse/[0-9a-zA-Z]+/'))
        if link:
            href = link.get('href', '')
            id_match = re.search(r'/horse/([0-9a-zA-Z]+)/', href)
            if id_match:
                return {
                    'id': id_match.group(1),
                    'name': link.get_text(strip=True)
                }
        return None
    
    @staticmethod
    def parse_date(text: str) -> Optional[str]:
        """YYYY年M月D日 → YYYY-MM-DD形式に変換"""
        date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text)
        if date_match:
            year, month, day = date_match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return None
    
    @staticmethod
    def parse_prize(text: str) -> int:
        """獲得賞金をパース（万円単位）"""
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
    
    @staticmethod
    def parse_career_record(text: str) -> Optional[Dict]:
        """通算成績をパース: 10戦8勝 [8-2-0-0]"""
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
        return None
    
    @staticmethod
    def parse_offering_info(text: str) -> Optional[Dict]:
        """募集情報をパース: 1口:8万円/500口"""
        offering_match = re.search(r'1口:(\d+)万円/(\d+)口', text)
        if offering_match:
            return {
                'price_per_unit': int(offering_match.group(1)),
                'total_units': int(offering_match.group(2)),
                'raw_text': text
            }
        elif text and text != '-':
            return {'raw_text': text}
        return None
    
    @staticmethod
    def clean_text(cell) -> Optional[str]:
        """テキストをクリーニング"""
        text = cell.get_text(strip=True) if hasattr(cell, 'get_text') else str(cell).strip()
        return text if text and text != '-' else None