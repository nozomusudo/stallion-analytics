import time
import re
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
import urllib.parse

from .base_scraper import BaseScraper
from ...database.schemas.jockey_schema import Jockey


class JockeyScraper(BaseScraper):
    """競馬騎手情報スクレイピングクラス"""
    
    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: リクエスト間のディレイ（秒）
        """
        super().__init__()
        self.delay = delay
    
    def scrape_jockeys(self, limit: int = 100, range_type: str = "all") -> List[Jockey]:
        """
        騎手一覧をスクレイピング
        
        Args:
            limit: 取得する騎手数（20, 50, 100の倍数推奨）
            range_type: 範囲指定（"all", "1", "2"など）
        
        Returns:
            騎手オブジェクトのリスト
        """
        jockeys = []
        page = 1
        remaining = limit
        
        while remaining > 0:
            # 1ページ最大100件取得
            page_limit = min(remaining, 100)
            
            print(f"ページ {page} を取得中... (残り {remaining} 件)")
            
            page_jockeys = self._scrape_page(page, range_type)
            
            if not page_jockeys:
                print("データが見つからないか、最終ページに到達しました")
                break
            
            # 必要な分だけ取得
            jockeys.extend(page_jockeys[:page_limit])
            remaining -= len(page_jockeys[:page_limit])
            
            # 次のページがない場合は終了
            if len(page_jockeys) < 100:
                break
            
            page += 1
            time.sleep(self.delay)
        
        print(f"合計 {len(jockeys)} 名の騎手データを取得しました")
        return jockeys
    
    def scrape(self, target_id: str) -> Optional[Jockey]:
        """
        BaseScraper の抽象メソッド実装
        単一の騎手データを取得
        """
        # target_id は騎手ID として解釈
        jockey_url = f"{self.base_url}/jockey/{target_id}/"
        soup = self.get_soup(jockey_url)
        
        if not soup:
            return None
        
        # 詳細ページから騎手データを解析
        # 現在は一覧ページメソッドを優先するため、簡易実装
        print(f"騎手詳細ページの解析は未実装です: {target_id}")
        return None
    
    def _scrape_page(self, page: int, range_type: str) -> List[Jockey]:
        """単一ページの騎手データをスクレイピング"""
        params = {
            "type": "",
            "word": "",
            "match": "p",
            "range": range_type,
            "state": "all",
            "sort": "rank-asc",
            "limit": "100",  # URLクエリは常に100固定
            "page": str(page)
        }
        
        # URLを構築
        query_string = urllib.parse.urlencode(params)
        url = f"{self.base_url}/jockey/list.html?{query_string}"
        
        soup = self.get_soup(url)
        if not soup:
            print(f"ページ {page} の取得に失敗しました")
            return []
        
        return self._parse_jockey_table(soup)
    
    def _parse_jockey_table(self, soup) -> List[Jockey]:
        """HTMLテーブルから騎手データを解析"""
        jockeys = []
        
        # テーブルを探す
        table = soup.find('table', class_='nk_tb_common race_table_01')
        if not table:
            print("騎手テーブルが見つかりませんでした")
            return jockeys
        
        # 全てのtr要素を取得（tbodyがない構造）
        all_rows = table.find_all('tr')
        if len(all_rows) < 3:
            print("データ行が見つかりませんでした")
            return jockeys
        
        # データ行を取得（最初の2行はヘッダー）
        rows = all_rows[2:]
        
        for i, row in enumerate(rows):
            try:
                jockey = self._parse_jockey_row(row)
                if jockey:
                    jockeys.append(jockey)
            except Exception as e:
                print(f"行 {i+1} の騎手データ解析に失敗: {e}")
                continue
        
        print(f"このページで {len(jockeys)} 名の騎手を取得しました")
        return jockeys
    
    def _parse_jockey_row(self, row) -> Optional[Jockey]:
        """テーブルの1行から騎手データを解析"""
        cells = row.find_all('td')
        if len(cells) < 19:  # 期待される列数より少ない場合はスキップ
            return None
        
        # 騎手名とIDを取得
        name_cell = cells[0].find('a')
        if not name_cell:
            return None
        
        name_ja = name_cell.get_text(strip=True)
        href = name_cell.get('href', '')
        jockey_id = self._extract_jockey_id(href)
        
        if not jockey_id:
            return None
        
        # 所属地域を取得（[東]、[西]など）
        region_text = cells[1].get_text(strip=True)
        region = self._extract_region(region_text)
        trainer_name = self._extract_trainer_name(cells[1])
        
        # 生年月日
        birthdate = self._parse_date(cells[2].get_text(strip=True))
        
        # 成績データ
        wins = self._parse_int(cells[3].get_text(strip=True))
        seconds = self._parse_int(cells[4].get_text(strip=True))
        thirds = self._parse_int(cells[5].get_text(strip=True))
        fourths = self._parse_int(cells[6].get_text(strip=True))
        
        # 総出走回数を計算
        total_races = wins + seconds + thirds + fourths
        
        # 条件別成績（重賞、特別、平場）
        grade_entries = self._parse_int(cells[7].get_text(strip=True))
        grade_wins = self._parse_int(cells[8].get_text(strip=True))
        special_entries = self._parse_int(cells[9].get_text(strip=True))
        special_wins = self._parse_int(cells[10].get_text(strip=True))
        normal_entries = self._parse_int(cells[11].get_text(strip=True))
        normal_wins = self._parse_int(cells[12].get_text(strip=True))
        
        # 馬場別成績（芝、ダート）
        turf_entries = self._parse_int(cells[13].get_text(strip=True))
        turf_wins = self._parse_int(cells[14].get_text(strip=True))
        dirt_entries = self._parse_int(cells[15].get_text(strip=True))
        dirt_wins = self._parse_int(cells[16].get_text(strip=True))
        
        # 勝率、連対率、複勝率
        win_rate = self._parse_decimal(cells[17].get_text(strip=True).replace('%', ''))
        # 連対率は18列目
        second_rate = self._parse_decimal(cells[18].get_text(strip=True).replace('%', ''))
        show_rate = self._parse_decimal(cells[19].get_text(strip=True).replace('%', ''))
        
        # 獲得賞金（万円）
        prize_money_text = cells[20].get_text(strip=True).replace(',', '')
        total_prize_money = self._parse_decimal(prize_money_text)
        if total_prize_money:
            total_prize_money = total_prize_money * 10000  # 万円を円に変換
        
        # 代表馬
        representative_horse = cells[21].get_text(strip=True) if len(cells) > 21 else None
        
        # JSONB用統計データを構築
        race_stats = {
            "grade": {"entries": grade_entries, "wins": grade_wins},
            "special": {"entries": special_entries, "wins": special_wins},
            "normal": {"entries": normal_entries, "wins": normal_wins}
        }
        
        track_stats = {
            "turf": {"entries": turf_entries, "wins": turf_wins},
            "dirt": {"entries": dirt_entries, "wins": dirt_wins}
        }
        
        # その他の統計データ
        yearly_stats = {
            "2025": {
                "races": total_races,
                "wins": wins,
                "seconds": seconds,
                "thirds": thirds,
                "representative_horse": representative_horse
            }
        }
        
        # Jockeyオブジェクトを作成
        jockey = Jockey(
            jockey_id=jockey_id,
            name_ja=name_ja,
            region=region,
            trainer_name=trainer_name,
            birthdate=birthdate,
            total_races=total_races,
            wins=wins,
            seconds=seconds,
            thirds=thirds,
            win_rate=win_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            yearly_stats=yearly_stats,
            track_stats=track_stats,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # 統計を自動計算
        jockey.update_stats()
        
        return jockey
    
    def _extract_jockey_id(self, href: str) -> Optional[str]:
        """HREFから騎手IDを抽出"""
        match = re.search(r'/jockey/(\d+)/', href)
        return match.group(1) if match else None
    
    def _extract_trainer_name(self, cell) -> Optional[str]:
        """所属セルから調教師名を抽出"""
        # リンクがある場合は調教師名
        trainer_link = cell.find('a')
        if trainer_link:
            return trainer_link.get_text(strip=True)
        
        # フリーの場合
        text = cell.get_text(strip=True)
        if 'フリー' in text:
            return 'フリー'
        
        # 地域表記を除去して残りのテキストを取得
        text = re.sub(r'\[.*?\]', '', text).strip()
        return text if text else None
    
    def _extract_region(self, text: str) -> Optional[str]:
        """所属テキストから地域を抽出"""
        if '[東]' in text:
            return '東'
        elif '[西]' in text:
            return '西'
        elif '[地方]' in text:
            return '地方'
        return None
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """日付文字列をdateオブジェクトに変換"""
        try:
            return datetime.strptime(date_str, '%Y/%m/%d').date()
        except (ValueError, AttributeError):
            return None
    
    def _parse_int(self, text: str) -> Optional[int]:
        """文字列を整数に変換"""
        try:
            cleaned = re.sub(r'[^\d]', '', text)
            return int(cleaned) if cleaned else 0
        except (ValueError, AttributeError):
            return 0
    
    def _parse_decimal(self, text: str) -> Optional[Decimal]:
        """文字列をDecimalに変換"""
        try:
            cleaned = re.sub(r'[^\d.]', '', text)
            return Decimal(cleaned) if cleaned else Decimal('0.0')
        except (ValueError, AttributeError, TypeError):
            return Decimal('0.0')


# 使用例
if __name__ == "__main__":
    scraper = JockeyScraper(delay=1.0)
    
    # 上位100名の騎手データを取得
    jockeys = scraper.scrape_jockeys(limit=100, range_type="all")
    
    # データの表示例
    for jockey in jockeys[:5]:
        print(f"騎手: {jockey.name_ja} ({jockey.jockey_id})")
        print(f"  所属: {jockey.region}")
        print(f"  生年月日: {jockey.birthdate}")
        print(f"  出走数: {jockey.total_races}, 勝利数: {jockey.wins}")
        print(f"  勝率: {jockey.win_rate}%, 複勝率: {jockey.show_rate}%")
        print(f"  獲得賞金: {jockey.total_prize_money:,}円")
        print(f"  調教師: {jockey.trainer_name}")
        print("  ---")
    
    print(f"\n合計取得数: {len(jockeys)}名")