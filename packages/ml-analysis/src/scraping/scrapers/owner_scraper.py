import time
import re
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
import urllib.parse

from .base_scraper import BaseScraper
from ...database.schemas.owner_schema import Owner


class OwnerScraper(BaseScraper):
    """競馬馬主情報スクレイピングクラス"""
    
    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: リクエスト間のディレイ（秒）
        """
        super().__init__()
        self.delay = delay
    
    def scrape_owners(self, limit: int = 100, range_type: str = "all") -> List[Owner]:
        """
        馬主一覧をスクレイピング
        
        Args:
            limit: 取得する馬主数（20, 50, 100の倍数推奨）
            range_type: 範囲指定（"all", "1", "2"など）
        
        Returns:
            馬主オブジェクトのリスト
        """
        owners = []
        page = 1
        remaining = limit
        
        while remaining > 0:
            # 1ページ最大100件取得
            page_limit = min(remaining, 100)
            
            print(f"ページ {page} を取得中... (残り {remaining} 件)")
            
            page_owners = self._scrape_page(page, range_type)
            
            # 必要な分だけ取得
            owners.extend(page_owners[:page_limit])
            remaining -= len(page_owners[:page_limit])
            
            # 次のページがない場合は終了
            if len(page_owners) == 0:
                break
            
            page += 1
            time.sleep(self.delay)
        
        print(f"合計 {len(owners)} 名の馬主データを取得しました")
        return owners
    
    def scrape(self, target_id: str) -> Optional[Owner]:
        """
        BaseScraper の抽象メソッド実装
        単一の馬主データを取得
        """
        # target_id は馬主ID として解釈
        owner_url = f"{self.base_url}/owner/{target_id}/"
        soup = self.get_soup(owner_url)
        
        if not soup:
            return None
        
        # 詳細ページから馬主データを解析
        # 現在は一覧ページメソッドを優先するため、簡易実装
        print(f"馬主詳細ページの解析は未実装です: {target_id}")
        return None
    
    def _scrape_page(self, page: int, range_type: str) -> List[Owner]:
        """単一ページの馬主データをスクレイピング"""
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
        
        # URLを構築（ownerに変更）
        query_string = urllib.parse.urlencode(params)
        url = f"{self.base_url}/owner/list.html?{query_string}"
        
        soup = self.get_soup(url)
        if not soup:
            print(f"ページ {page} の取得に失敗しました")
            return []
        
        return self._parse_owner_table(soup)
    
    def _parse_owner_table(self, soup) -> List[Owner]:
        """HTMLテーブルから馬主データを解析"""
        owners = []
        
        # テーブルを探す
        table = soup.find('table', class_='nk_tb_common race_table_01')
        if not table:
            print("馬主テーブルが見つかりませんでした")
            return owners
        
        # 全てのtr要素を取得（tbodyがない構造）
        all_rows = table.find_all('tr')
        if len(all_rows) < 3:
            print("データ行が見つかりませんでした")
            return owners
        
        # データ行を取得（最初の2行はヘッダー）
        rows = all_rows[2:]
        
        for i, row in enumerate(rows):
            try:
                owner = self._parse_owner_row(row)
                if owner:
                    owners.append(owner)
            except Exception as e:
                print(f"行 {i+1} の馬主データ解析に失敗: {e}")
                continue
        
        print(f"このページで {len(owners)} 名の馬主を取得しました")
        return owners
    
    def _parse_owner_row(self, row) -> Optional[Owner]:
        """テーブルの1行から馬主データを解析"""
        cells = row.find_all('td')
        if len(cells) < 20:  # 期待される列数より少ない場合はスキップ（馬主は20列）
            return None
        
        # 馬主名とIDを取得
        name_cell = cells[0].find('a')
        if not name_cell:
            return None
        
        name_ja = name_cell.get_text(strip=True)
        href = name_cell.get('href', '')
        owner_id = self._extract_owner_id(href)
        
        if not owner_id:
            return None
        
        # 成績データ（調教師と同じ順序）
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
        
        # 馬主特有の統計を計算
        stakes_wins = grade_wins + special_wins  # 重賞＋特別レース勝利
        # G1勝利数は詳細データがないため0に設定（後で詳細ページから取得可能）
        grade1_wins = 0
        
        # 所有馬数は一覧ページからは取得できないため、推定値を設定
        # 出走数から大まかに推定（1頭あたり年間平均出走数を仮定）
        estimated_horses = max(1, total_races // 8) if total_races > 0 else 1
        
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
                "estimated_horses": estimated_horses
            }
        }
        
        # 馬主の種類を名前から推定（簡易判定）
        owner_type = self._estimate_owner_type(name_ja)
        
        # Ownerオブジェクトを作成
        owner = Owner(
            owner_id=owner_id,
            name_ja=name_ja,
            owner_type=owner_type,
            total_races=total_races,
            wins=wins,
            seconds=seconds,
            thirds=thirds,
            win_rate=win_rate,
            second_rate=second_rate,
            show_rate=show_rate,
            total_prize_money=total_prize_money,
            total_horses=estimated_horses,
            active_horses=estimated_horses,  # 現役馬数は推定値
            retired_horses=0,
            stakes_wins=stakes_wins,
            grade1_wins=grade1_wins,
            yearly_stats=yearly_stats,
            race_stats=race_stats,
            track_stats=track_stats,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # 統計を自動計算
        owner.update_stats()
        
        return owner
    
    def _extract_owner_id(self, href: str) -> Optional[str]:
        """HREFから馬主IDを抽出"""
        match = re.search(r'/owner/(\d+)/', href)
        return match.group(1) if match else None
    
    def _estimate_owner_type(self, name: str) -> str:
        """馬主名から馬主タイプを推定"""
        # 法人系キーワード
        corporate_keywords = [
            '株式会社', '有限会社', '合同会社', '合名会社', '合資会社',
            'ホールディングス', 'レーシング', 'ファーム', 'クラブ',
            '組合', '事業組合', '競走馬組合'
        ]
        
        # 組合系キーワード  
        partnership_keywords = ['組合', '事業組合', '競走馬組合']
        
        name_lower = name.lower()
        
        # 組合の判定を先に行う
        if any(keyword in name for keyword in partnership_keywords):
            return 'partnership'
        
        # 法人の判定
        if any(keyword in name for keyword in corporate_keywords):
            return 'corporation'
        
        # デフォルトは個人
        return 'individual'
    
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
    scraper = OwnerScraper(delay=1.0)
    
    # 全馬主データを取得（上限1000件で実質全件）
    owners = scraper.scrape_owners(limit=1000, range_type="all")
    
    # データの表示例
    for owner in owners[:5]:
        print(f"馬主: {owner.name_ja} ({owner.owner_id})")
        print(f"  タイプ: {owner.owner_type}")
        print(f"  出走数: {owner.total_races}, 勝利数: {owner.wins}")
        print(f"  勝率: {owner.win_rate}%, 複勝率: {owner.show_rate}%")
        print(f"  獲得賞金: {owner.total_prize_money:,}円")
        print(f"  重賞勝利: {owner.stakes_wins}勝")
        print(f"  推定所有馬数: {owner.total_horses}頭")
        print(f"  代表馬: {owner.yearly_stats.get('2025', {}).get('representative_horse', 'N/A')}")
        print("  ---")
    
    print(f"\n合計取得数: {len(owners)}名")