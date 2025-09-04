import time
import re
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
import urllib.parse

from .base_scraper import BaseScraper
from ...database.schemas.breeder_schema import Breeder


class BreederScraper(BaseScraper):
    """競馬生産者情報スクレイピングクラス"""
    
    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: リクエスト間のディレイ（秒）
        """
        super().__init__()
        self.delay = delay
    
    def scrape_breeders(self, limit: int = 100, range_type: str = "all") -> List[Breeder]:
        """
        生産者一覧をスクレイピング
        
        Args:
            limit: 取得する生産者数（20, 50, 100の倍数推奨）
            range_type: 範囲指定（"all", "1", "2"など）
        
        Returns:
            生産者オブジェクトのリスト
        """
        breeders = []
        page = 1
        remaining = limit
        
        while remaining > 0:
            # 1ページ最大100件取得
            page_limit = min(remaining, 100)
            
            print(f"ページ {page} を取得中... (残り {remaining} 件)")
            
            page_breeders = self._scrape_page(page, range_type)
            
            # 必要な分だけ取得
            breeders.extend(page_breeders[:page_limit])
            remaining -= len(page_breeders[:page_limit])
            
            # 次のページがない場合は終了
            if len(page_breeders) == 0:
                break
            
            page += 1
            time.sleep(self.delay)
        
        print(f"合計 {len(breeders)} 名の生産者データを取得しました")
        return breeders
    
    def scrape(self, target_id: str) -> Optional[Breeder]:
        """
        BaseScraper の抽象メソッド実装
        単一の生産者データを取得
        """
        # target_id は生産者ID として解釈
        breeder_url = f"{self.base_url}/breeder/{target_id}/"
        soup = self.get_soup(breeder_url)
        
        if not soup:
            return None
        
        # 詳細ページから生産者データを解析
        # 現在は一覧ページメソッドを優先するため、簡易実装
        print(f"生産者詳細ページの解析は未実装です: {target_id}")
        return None
    
    def _scrape_page(self, page: int, range_type: str) -> List[Breeder]:
        """単一ページの生産者データをスクレイピング"""
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
        
        # URLを構築（breederに変更）
        query_string = urllib.parse.urlencode(params)
        url = f"{self.base_url}/breeder/list.html?{query_string}"
        
        soup = self.get_soup(url)
        if not soup:
            print(f"ページ {page} の取得に失敗しました")
            return []
        
        return self._parse_breeder_table(soup)
    
    def _parse_breeder_table(self, soup) -> List[Breeder]:
        """HTMLテーブルから生産者データを解析"""
        breeders = []
        
        # テーブルを探す
        table = soup.find('table', class_='nk_tb_common race_table_01')
        if not table:
            print("生産者テーブルが見つかりませんでした")
            return breeders
        
        # 全てのtr要素を取得（tbodyがない構造）
        all_rows = table.find_all('tr')
        if len(all_rows) < 3:
            print("データ行が見つかりませんでした")
            return breeders
        
        # データ行を取得（最初の2行はヘッダー）
        rows = all_rows[2:]
        
        for i, row in enumerate(rows):
            try:
                breeder = self._parse_breeder_row(row)
                if breeder:
                    breeders.append(breeder)
            except Exception as e:
                print(f"行 {i+1} の生産者データ解析に失敗: {e}")
                continue
        
        print(f"このページで {len(breeders)} 名の生産者を取得しました")
        return breeders
    
    def _parse_breeder_row(self, row) -> Optional[Breeder]:
        """テーブルの1行から生産者データを解析"""
        cells = row.find_all('td')
        if len(cells) < 20:  # 期待される列数より少ない場合はスキップ（生産者は20列）
            return None
        
        # 生産者名とIDを取得
        name_cell = cells[0].find('a')
        if not name_cell:
            return None
        
        name_ja = name_cell.get_text(strip=True)
        href = name_cell.get('href', '')
        breeder_id = self._extract_breeder_id(href)
        
        if not breeder_id:
            return None
        
        # 成績データ
        wins = self._parse_int(cells[1].get_text(strip=True))
        seconds = self._parse_int(cells[2].get_text(strip=True))
        thirds = self._parse_int(cells[3].get_text(strip=True))
        fourths = self._parse_int(cells[4].get_text(strip=True))
        
        # 総出走回数を計算
        total_races = wins + seconds + thirds + fourths
        
        # 条件別成績（重賞、特別、平場）
        grade_entries = self._parse_int(cells[5].get_text(strip=True))
        grade_wins = self._parse_int(cells[6].get_text(strip=True))
        special_entries = self._parse_int(cells[7].get_text(strip=True))
        special_wins = self._parse_int(cells[8].get_text(strip=True))
        normal_entries = self._parse_int(cells[9].get_text(strip=True))
        normal_wins = self._parse_int(cells[10].get_text(strip=True))
        
        # 馬場別成績（芝、ダート）
        turf_entries = self._parse_int(cells[11].get_text(strip=True))
        turf_wins = self._parse_int(cells[12].get_text(strip=True))
        dirt_entries = self._parse_int(cells[13].get_text(strip=True))
        dirt_wins = self._parse_int(cells[14].get_text(strip=True))
        
        # 勝率、連対率、複勝率
        win_rate = self._parse_decimal(cells[15].get_text(strip=True).replace('%', ''))
        second_rate = self._parse_decimal(cells[16].get_text(strip=True).replace('%', ''))
        show_rate = self._parse_decimal(cells[17].get_text(strip=True).replace('%', ''))
        
        # 獲得賞金（万円）
        prize_money_text = cells[18].get_text(strip=True).replace(',', '')
        total_prize_money = self._parse_decimal(prize_money_text)
        if total_prize_money:
            total_prize_money = total_prize_money * 10000  # 万円を円に変換
        
        # 代表馬
        representative_horse = cells[19].get_text(strip=True) if len(cells) > 19 else None
        
        # 生産者特有の統計を計算
        stakes_wins = grade_wins + special_wins  # 重賞＋特別レース勝利
        # G1勝利数は詳細データがないため0に設定（後で詳細ページから取得可能）
        grade1_wins = 0
        
        # 生産頭数は一覧ページからは取得できないため、推定値を設定
        # 出走数から大まかに推定（1頭あたり年間平均出走数を仮定）
        estimated_produced = max(1, total_races // 6) if total_races > 0 else 1
        estimated_debut = max(1, total_races // 8) if total_races > 0 else 1
        
        # 生産者タイプを名前から推定（簡易判定）
        breeder_type = self._estimate_breeder_type(name_ja)
        
        # 所在地を名前から推定（北海道が多いため）
        location = self._estimate_location(name_ja)
        
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
                "representative_horse": representative_horse,
                "estimated_produced": estimated_produced
            }
        }
        
        # Breederオブジェクトを作成
        breeder = Breeder(
            breeder_id=breeder_id,
            name_ja=name_ja,
            breeder_type=breeder_type,
            location=location,
            total_races=total_races,
            wins=wins,
            seconds=seconds,
            thirds=thirds,
            win_rate=win_rate,
            second_rate=second_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            total_horses_produced=estimated_produced,
            active_horses=max(1, total_races // 10) if total_races > 0 else 1,
            retired_horses=0,
            stakes_wins=stakes_wins,
            grade1_wins=grade1_wins,
            debut_horses=estimated_debut,
            yearly_stats=yearly_stats,
            race_stats=race_stats,
            track_stats=track_stats,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # 統計を自動計算
        breeder.update_stats()
        
        return breeder
    
    def _extract_breeder_id(self, href: str) -> Optional[str]:
        """HREFから生産者IDを抽出"""
        match = re.search(r'/breeder/(\d+)/', href)
        return match.group(1) if match else None
    
    def _estimate_breeder_type(self, name: str) -> str:
        """生産者名から生産者タイプを推定"""
        # 牧場系キーワード
        farm_keywords = [
            'ファーム', 'ステーション', '牧場', 'スタッド', 'レーシング',
            'ブリーディング', 'ホース'
        ]
        
        # 法人系キーワード
        corporate_keywords = [
            '株式会社', '有限会社', '合同会社', '合名会社', '合資会社',
            'ホールディングス', '事業組合', '組合'
        ]
        
        # 牧場の判定を先に行う（ファームが付いていることが多い）
        if any(keyword in name for keyword in farm_keywords):
            return 'farm'
        
        # 法人の判定
        if any(keyword in name for keyword in corporate_keywords):
            return 'corporation'
        
        # デフォルトは個人
        return 'individual'
    
    def _estimate_location(self, name: str) -> Optional[str]:
        """生産者名から所在地を推定（簡易版）"""
        # 地域名が含まれている場合
        location_keywords = {
            '日高': '北海道',
            '新冠': '北海道', 
            '新ひだか': '北海道',
            '浦河': '北海道',
            '静内': '北海道',
            '門別': '北海道',
            '千歳': '北海道',
            '札幌': '北海道',
            '函館': '北海道',
            '栃木': '栃木県',
            '宮崎': '宮崎県',
            '熊本': '熊本県'
        }
        
        for keyword, location in location_keywords.items():
            if keyword in name:
                return location
        
        # 大多数の生産者は北海道のため、デフォルトは北海道
        return '北海道'
    
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
    scraper = BreederScraper(delay=1.0)
    
    # 全生産者データを取得（上限1000件で実質全件）
    breeders = scraper.scrape_breeders(limit=1000, range_type="all")
    
    # データの表示例
    for breeder in breeders[:5]:
        print(f"生産者: {breeder.name_ja} ({breeder.breeder_id})")
        print(f"  タイプ: {breeder.breeder_type}")
        print(f"  所在地: {breeder.location}")
        print(f"  出走数: {breeder.total_races}, 勝利数: {breeder.wins}")
        print(f"  勝率: {breeder.win_rate}%, 複勝率: {breeder.show_rate}%")
        print(f"  獲得賞金: {breeder.total_prize_money:,}円")
        print(f"  重賞勝利: {breeder.stakes_wins}勝")
        print(f"  推定生産頭数: {breeder.total_horses_produced}頭")
        print(f"  代表馬: {breeder.yearly_stats.get('2025', {}).get('representative_horse', 'N/A')}")
        print("  ---")
    
    print(f"\n合計取得数: {len(breeders)}名")