# src/scraping/scrapers/base_scraper.py

import requests
from bs4 import BeautifulSoup
import time
from typing import Optional
from abc import ABC, abstractmethod

class BaseScraper(ABC):
    """競馬スクレイピングの基底クラス"""
    
    def __init__(self, base_url: str = "https://db.netkeiba.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """URLからBeautifulSoupオブジェクトを取得"""
        try:
            print(f"🔍 ページ取得: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
            
        except Exception as e:
            print(f"❌ ページ取得エラー: {e}")
            return None
    
    def sleep(self, seconds: int = 1):
        """リクエスト間隔制御"""
        time.sleep(seconds)
    
    @abstractmethod
    def scrape(self, target_id: str):
        """サブクラスで実装する抽出メソッド"""
        pass