import requests
from bs4 import BeautifulSoup
import time
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# 環境変数を読み込み
load_dotenv()

class HorseListScraper:
    def __init__(self):
        """G1馬スクレイピングクラスの初期化"""
        self.base_url = "https://db.netkeiba.com/horse/list.html"
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

    def scrape_g1_horses(self, max_horses: int = 5, min_birth_year: int = 2018) -> List[Dict]:
        """
        G1勝利馬の基本情報をスクレイピング
        
        Args:
            max_horses: 取得する最大馬数
            min_birth_year: 取得対象の最小生年（この年以降生まれ）
        
        Returns:
            馬の情報リスト
        """
        horses = []
        page = 1
        
        print(f"🐎 G1馬を最大{max_horses}頭、{min_birth_year}年以降生まれから取得開始...")
        
        while len(horses) < max_horses:
            print(f"📄 ページ{page}を処理中...")
            
            # リクエストパラメータ
            params = {
                "grade[]": "4",           # G1勝利馬
                "sort": "age-desc",       # 生年降順
                "limit": "100",           # 100件/ページ
                "page": page
            }
            
            try:
                response = self.session.get(self.base_url, params=params)
                response.raise_for_status()
                
                # netkeibaはEUC-JPエンコーディングを使用
                response.encoding = 'euc-jp'
                
                soup = BeautifulSoup(response.text, 'html.parser')
                page_horses = self._parse_horse_list(soup, min_birth_year)
                
                if not page_horses:
                    print(f"✅ ページ{page}で対象馬が見つかりませんでした。終了します。")
                    break
                
                # 必要な頭数まで追加
                for horse in page_horses:
                    if len(horses) < max_horses:
                        horses.append(horse)
                        print(f"  ✓ {horse['name_ja']} ({horse['birth_year']}年生)")
                    else:
                        break
                
                page += 1
                time.sleep(1)  # サーバー負荷軽減
                
            except requests.RequestException as e:
                print(f"❌ ページ{page}の取得でエラー: {e}")
                break
        
        print(f"🎯 合計{len(horses)}頭の馬を取得しました")
        return horses

    def _parse_horse_list(self, soup: BeautifulSoup, min_birth_year: int) -> List[Dict]:
        """リストページのHTMLから馬情報を抽出"""
        horses = []
        
        # テーブル内の各行を解析
        for row in soup.find_all('tr'):
            try:
                # チェックボックスから馬IDを取得
                checkbox = row.find('input', {'type': 'checkbox'})
                if not checkbox or 'i-horse_' not in checkbox.get('name', ''):
                    continue
                
                horse_id = checkbox.get('value')
                
                # 各tdを取得
                tds = row.find_all('td')
                if len(tds) < 4:
                    continue
                
                # 馬名を取得（2番目のtd）
                name_cell = tds[1].find('a')
                if not name_cell:
                    continue
                name_ja = name_cell.text.strip()
                
                # 性別を取得（3番目のtd）
                sex = tds[2].text.strip()
                
                # 生年を取得（4番目のtd）
                birth_year_link = tds[3].find('a')
                if not birth_year_link:
                    continue
                birth_year = int(birth_year_link.text.strip())
                
                # 生年フィルタリング
                if birth_year < min_birth_year:
                    continue
                
                horse_info = {
                    'id': int(horse_id),
                    'name_ja': name_ja,
                    'sex': sex,
                    'birth_year': birth_year
                }
                
                horses.append(horse_info)
                
            except (ValueError, AttributeError) as e:
                # パースエラーは無視して次の行へ
                continue
        
        return horses

    def save_to_supabase(self, horses: List[Dict]) -> bool:
        """Supabaseにデータを保存"""
        if not self.supabase:
            print("❌ Supabaseクライアントが初期化されていません")
            return False
        
        if not horses:
            print("❌ 保存するデータがありません")
            return False
        
        try:
            print(f"💾 Supabaseに{len(horses)}頭のデータを保存中...")
            
            # 保存用データを準備（name_jaのみ）
            insert_data = [
                {
                    'id': horse['id'],
                    'name_ja': horse['name_ja']
                }
                for horse in horses
            ]
            
            # upsert（挿入または更新）で重複を避ける
            result = self.supabase.table('horses').upsert(
                insert_data,
                on_conflict='id'  # IDが重複した場合は更新
            ).execute()
            
            print(f"✅ {len(result.data)}件のデータを保存しました")
            return True
            
        except Exception as e:
            print(f"❌ Supabase保存エラー: {e}")
            return False

    def run(self, max_horses: int = 5, min_birth_year: int = 2018, save_to_db: bool = True) -> List[Dict]:
        """
        スクレイピング実行のメインメソッド
        
        Args:
            max_horses: 取得する最大馬数
            min_birth_year: 対象生年（この年以降）
            save_to_db: Supabaseに保存するかどうか
        
        Returns:
            取得した馬のリスト
        """
        print("🏇 G1馬スクレイピング開始")
        print(f"📋 設定: 最大{max_horses}頭、{min_birth_year}年以降生まれ")
        
        # スクレイピング実行
        horses = self.scrape_g1_horses(max_horses, min_birth_year)
        
        if not horses:
            print("😅 該当する馬が見つかりませんでした")
            return horses
        
        # 結果表示
        print("\n📊 取得結果:")
        for horse in horses:
            print(f"  ID: {horse['id']}, 名前: {horse['name_ja']}, 生年: {horse['birth_year']}")
        
        # Supabaseに保存
        if save_to_db:
            success = self.save_to_supabase(horses)
            if success:
                print("🎉 すべての処理が完了しました!")
            else:
                print("⚠️ データベース保存に失敗しましたが、スクレイピングは成功しました")
        
        return horses


# 使用例
if __name__ == "__main__":
    scraper = HorseListScraper()
    
    # テスト実行（5頭のみ）
    horses = scraper.run(
        max_horses=5,
        min_birth_year=2018,
        save_to_db=True
    )
    
    # より多くの馬を取得する場合
    # horses = scraper.run(max_horses=50, min_birth_year=2018)