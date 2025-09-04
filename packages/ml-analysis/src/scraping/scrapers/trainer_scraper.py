import time
import re
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
import urllib.parse

from .base_scraper import BaseScraper
from ...database.schemas.trainer_schema import Trainer


class TrainerScraper(BaseScraper):
    """競馬調教師情報スクレイピングクラス"""
    
    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: リクエスト間のディレイ（秒）
        """
        super().__init__()
        self.delay = delay
    
    def scrape_trainers(self, limit: int = 100, range_type: str = "all") -> List[Trainer]:
        """
        調教師一覧をスクレイピング
        
        Args:
            limit: 取得する調教師数（20, 50, 100の倍数推奨）
            range_type: 範囲指定（"all", "1", "2"など）
        
        Returns:
            調教師オブジェクトのリスト
        """
        trainers = []
        page = 1
        remaining = limit
        
        while remaining > 0:
            # 1ページ最大100件取得
            page_limit = min(remaining, 100)
            
            print(f"ページ {page} を取得中... (残り {remaining} 件)")
            
            page_trainers = self._scrape_page(page, range_type)
            
            """
            if not page_trainers:
                print("データが見つからないか、最終ページに到達しました")
                break
            """
                
            # 必要な分だけ取得
            trainers.extend(page_trainers[:page_limit])
            remaining -= len(page_trainers[:page_limit])
            
            # 次のページがない場合は終了
            # if len(page_trainers) < 100:
            if len(page_trainers) == 0:
                break
            
            page += 1
            time.sleep(self.delay)
        
        print(f"合計 {len(trainers)} 名の調教師データを取得しました")
        return trainers
    
    def scrape(self, target_id: str) -> Optional[Trainer]:
        """
        BaseScraper の抽象メソッド実装
        単一の調教師データを取得
        """
        # target_id は調教師ID として解釈
        trainer_url = f"{self.base_url}/trainer/{target_id}/"
        soup = self.get_soup(trainer_url)
        
        if not soup:
            return None
        
        # 詳細ページから調教師データを解析
        # 現在は一覧ページメソッドを優先するため、簡易実装
        print(f"調教師詳細ページの解析は未実装です: {target_id}")
        return None
    
    def _scrape_page(self, page: int, range_type: str) -> List[Trainer]:
        """単一ページの調教師データをスクレイピング"""
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
        url = f"{self.base_url}/trainer/list.html?{query_string}"
        
        soup = self.get_soup(url)
        if not soup:
            print(f"ページ {page} の取得に失敗しました")
            return []
        
        return self._parse_trainer_table(soup)
    
    def _parse_trainer_table(self, soup) -> List[Trainer]:
        """HTMLテーブルから調教師データを解析"""
        trainers = []
        
        # テーブルを探す
        table = soup.find('table', class_='nk_tb_common race_table_01')
        if not table:
            print("調教師テーブルが見つかりませんでした")
            return trainers
        
        # 全てのtr要素を取得（tbodyがない構造）
        all_rows = table.find_all('tr')
        if len(all_rows) < 3:
            print("データ行が見つかりませんでした")
            return trainers
        
        # データ行を取得（最初の2行はヘッダー）
        rows = all_rows[2:]
        
        for i, row in enumerate(rows):
            try:
                trainer = self._parse_trainer_row(row)
                if trainer:
                    trainers.append(trainer)
            except Exception as e:
                print(f"行 {i+1} の調教師データ解析に失敗: {e}")
                continue
        
        print(f"このページで {len(trainers)} 名の調教師を取得しました")
        return trainers
    
    def _parse_trainer_row(self, row) -> Optional[Trainer]:
        """テーブルの1行から調教師データを解析"""
        cells = row.find_all('td')
        if len(cells) < 22:  # 期待される列数より少ない場合はスキップ
            return None
        
        # 調教師名とIDを取得
        name_cell = cells[0].find('a')
        if not name_cell:
            return None
        
        name_ja = name_cell.get_text(strip=True)
        href = name_cell.get('href', '')
        trainer_id = self._extract_trainer_id(href)
        
        if not trainer_id:
            return None
        
        # 所属地域を取得（美浦、栗東など）
        region = cells[1].get_text(strip=True)
        
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
        
        # Trainerオブジェクトを作成
        trainer = Trainer(
            trainer_id=trainer_id,
            name_ja=name_ja,
            region=region,
            birthdate=birthdate,
            total_races=total_races,
            wins=wins,
            seconds=seconds,
            thirds=thirds,
            win_rate=win_rate,
            second_rate=second_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            yearly_stats=yearly_stats,
            race_stats=race_stats,
            track_stats=track_stats,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # 統計を自動計算
        trainer.update_stats()
        
        return trainer
    
    def _extract_trainer_id(self, href: str) -> Optional[str]:
        """HREFから調教師IDを抽出"""
        match = re.search(r'/trainer/(\d+)/', href)
        return match.group(1) if match else None
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """日付文字列をdateオブジェクトに変換"""
        try:
            return datetime.strptime(date_str, '%Y/%m/%d').date()
        except (ValueError, AttributeError):
            return None
    
    def _parse_int(self, text: str) -> Optional[int]:
        """文字列を整数に変換（空白・ハイフン・0対応）"""
        try:
            if not text or text.strip() in ['', '-', '0']:
                return 0
            cleaned = re.sub(r'[^\d]', '', text)
            return int(cleaned) if cleaned else 0
        except (ValueError, AttributeError):
            return 0
    
    def _parse_decimal(self, text: str) -> Optional[Decimal]:
        """文字列をDecimalに変換（空白・ハイフン・0%対応）"""
        try:
            if not text or text.strip() in ['', '-', '0%', '0']:
                return Decimal('0.0')
            cleaned = re.sub(r'[^\d.]', '', text)
            return Decimal(cleaned) if cleaned else Decimal('0.0')
        except (ValueError, AttributeError, TypeError):
            return Decimal('0.0')


# 使用例
if __name__ == "__main__":
    scraper = TrainerScraper(delay=1.0)
    
    # 全調教師データを取得（上限1000件で実質全件）
    trainers = scraper.scrape_trainers(limit=1000, range_type="all")
    
    # データの表示例
    for trainer in trainers[:5]:
        print(f"調教師: {trainer.name_ja} ({trainer.trainer_id})")
        print(f"  所属: {trainer.region}")
        print(f"  生年月日: {trainer.birthdate}")
        print(f"  出走数: {trainer.total_races}, 勝利数: {trainer.wins}")
        print(f"  勝率: {trainer.win_rate}%, 複勝率: {trainer.show_rate}%")
        print(f"  獲得賞金: {trainer.total_prize_money:,}円")
        print(f"  代表馬: {trainer.yearly_stats.get('2025', {}).get('representative_horse', 'N/A')}")
        print("  ---")
    
    print(f"\n合計取得数: {len(trainers)}名")