# シンプルなオフセット方式での馬リスト取得
# 確実に重複なく大量データを収集

import requests
from bs4 import BeautifulSoup
import time
import math
from typing import List, Dict, Optional
import json
import os
from datetime import datetime

class SimpleOffsetHorseListScraper:
    """シンプルなオフセット方式でG1馬リストを取得"""
    
    def __init__(self):
        self.base_url = "https://db.netkeiba.com/horse/list.html"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_g1_horses_by_offset(
        self, 
        offset: int = 0,
        max_horses: int = 100,
        min_birth_year: int = 2015,
        per_page: int = 100
    ) -> List[Dict]:
        """
        オフセット方式でG1馬を取得
        
        Args:
            offset: 開始位置（0から）
            max_horses: 取得する最大馬数
            min_birth_year: 最小生年（これより古い馬が出たら終了）
            per_page: 1ページあたりの馬数（100固定）
        
        Returns:
            馬のリスト
        """
        print(f"🎯 G1馬取得開始:")
        print(f"   オフセット: {offset}")
        print(f"   最大取得数: {max_horses}頭")
        print(f"   最小生年: {min_birth_year}年")
        print("-" * 50)
        
        # ページ計算
        start_page = (offset // per_page) + 1
        start_position = offset % per_page
        
        horses = []
        current_page = start_page
        horses_collected = 0
        position_in_page = start_position
        
        print(f"📄 開始ページ: {current_page}, ページ内位置: {start_position}")
        
        while horses_collected < max_horses:
            print(f"\n📄 ページ{current_page}を処理中...")
            
            # ページのデータを取得
            page_horses = self._get_page_horses(current_page, per_page)
            
            if not page_horses:
                print("❌ データ取得に失敗しました")
                break
            
            # ページ内の指定位置から処理開始
            for i in range(position_in_page, len(page_horses)):
                horse = page_horses[i]
                
                # 生年チェック
                if horse['birth_year'] < min_birth_year:
                    print(f"⏹️ 生年{horse['birth_year']}年の{horse['name_ja']}に到達。処理終了")
                    return horses
                
                horses.append(horse)
                horses_collected += 1
                
                print(f"  ✓ [{horses_collected:3d}] {horse['name_ja']} ({horse['birth_year']}年)")
                
                # 目標頭数に到達
                if horses_collected >= max_horses:
                    break
            
            # 次のページへ
            current_page += 1
            position_in_page = 0  # 次のページからは最初から
            
            # API負荷軽減
            time.sleep(1)
        
        print(f"\n✅ 合計{len(horses)}頭を取得しました")
        return horses
    
    def _get_page_horses(self, page: int, limit: int = 100) -> List[Dict]:
        """指定ページの馬リストを取得"""
        params = {
            "grade[]": "4",  # G1勝利馬
            "sort": "age-desc",  # 生年降順
            "limit": limit,
            "page": page
        }
        
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            response.encoding = 'euc-jp'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            return self._parse_horse_list(soup)
            
        except requests.RequestException as e:
            print(f"❌ ページ{page}取得エラー: {e}")
            return []
    
    def _parse_horse_list(self, soup: BeautifulSoup) -> List[Dict]:
        """ページのHTMLから馬情報を抽出"""
        horses = []
        
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
                
                horses.append({
                    'id': int(horse_id),
                    'name_ja': name_ja,
                    'sex': sex,
                    'birth_year': birth_year
                })
                
            except (ValueError, AttributeError):
                continue
        
        return horses


class CompleteBatchProcessor:
    """馬リスト取得 + 詳細データ取得の完全バッチ処理"""
    
    def __init__(self):
        self.list_scraper = SimpleOffsetHorseListScraper()
        # HorseScraperは動的インポート（循環インポート回避）
        self.detail_scraper = None
    
    def _init_detail_scraper(self):
        """HorseScraperの遅延初期化"""
        if self.detail_scraper is None:
            try:
                from scrapers.horse_scraper import HorseScraper
                self.detail_scraper = HorseScraper()
            except ImportError as e:
                print(f"⚠️ HorseScraperの読み込みに失敗: {e}")
                return False
        return True
    
    def collect_and_process_horses(
        self,
        total_target: int = 100,
        batch_size: int = 25,
        start_offset: int = 0,
        min_birth_year: int = 2015,
        process_details: bool = True,
        delay_between_horses: int = 3
    ) -> Dict:
        """
        馬リスト取得 + 詳細処理の完全バッチ
        
        Args:
            total_target: 目標総数
            batch_size: バッチサイズ
            start_offset: 開始オフセット
            min_birth_year: 最小生年
            process_details: 詳細データ処理するか
            delay_between_horses: 馬間の処理間隔
        
        Returns:
            処理結果
        """
        print("=" * 70)
        print("🏇 完全バッチ処理開始")
        print("=" * 70)
        print(f"🎯 目標: {total_target}頭")
        print(f"📦 バッチサイズ: {batch_size}頭")
        print(f"📍 開始オフセット: {start_offset}")
        print(f"📅 最小生年: {min_birth_year}年")
        print(f"🔧 詳細処理: {'有効' if process_details else '無効'}")
        print()
        
        # Step 1: 馬リスト取得
        print("🔍 Step 1: 馬リスト取得")
        horse_list = self._collect_horse_list(total_target, batch_size, start_offset, min_birth_year)
        
        if not horse_list:
            return {'error': '馬リスト取得に失敗'}
        
        # 結果保存
        results = {
            'horse_list': horse_list,
            'list_count': len(horse_list),
            'success': [],
            'failed': [],
            'total_processed': 0
        }
        
        # Step 2: 詳細データ処理（オプション）
        if process_details and horse_list:
            print(f"\n🔍 Step 2: 詳細データ処理（{len(horse_list)}頭）")
            
            if not self._init_detail_scraper():
                print("❌ 詳細処理をスキップします")
                process_details = False
            else:
                detail_results = self._process_horse_details(horse_list, delay_between_horses)
                results.update(detail_results)
        
        # 結果サマリー
        self._print_final_summary(results)
        
        # ファイル保存
        self._save_results(results)
        
        return results
    
    def _collect_horse_list(self, total_target: int, batch_size: int, start_offset: int, min_birth_year: int) -> List[Dict]:
        """馬リストの収集"""
        all_horses = []
        current_offset = start_offset
        
        while len(all_horses) < total_target:
            remaining = total_target - len(all_horses)
            current_batch_size = min(batch_size, remaining)
            
            print(f"📦 リストバッチ: オフセット{current_offset}, {current_batch_size}頭取得")
            
            batch_horses = self.list_scraper.get_g1_horses_by_offset(
                offset=current_offset,
                max_horses=current_batch_size,
                min_birth_year=min_birth_year
            )
            
            if not batch_horses:
                print("⚠️ これ以上馬が見つかりません")
                break
            
            # 重複チェック
            new_horses = []
            existing_ids = {horse['id'] for horse in all_horses}
            
            for horse in batch_horses:
                if horse['id'] not in existing_ids:
                    new_horses.append(horse)
            
            all_horses.extend(new_horses)
            current_offset += len(batch_horses)
            
            print(f"✅ リストバッチ完了: {len(new_horses)}頭追加, 累計{len(all_horses)}頭")
            
            if len(all_horses) < total_target:
                time.sleep(2)
        
        return all_horses
    
    def _process_horse_details(self, horse_list: List[Dict], delay: int) -> Dict:
        """馬の詳細データ処理"""
        results = {
            'success': [],
            'failed': [],
            'total_processed': 0
        }
        
        total = len(horse_list)
        
        for i, horse in enumerate(horse_list, 1):
            horse_id = str(horse['id'])
            horse_name = horse['name_ja']
            
            print(f"🐎 [{i}/{total}] {horse_name} (ID: {horse_id})")
            
            try:
                success = self.detail_scraper.scrape(horse_id)
                
                if success:
                    results['success'].append({
                        'id': horse_id,
                        'name': horse_name,
                        'processed_at': datetime.now().isoformat()
                    })
                    print(f"    ✅ 完了")
                else:
                    results['failed'].append({
                        'id': horse_id,
                        'name': horse_name,
                        'error': '詳細スクレイピング失敗'
                    })
                    print(f"    ❌ 失敗")
                
            except Exception as e:
                results['failed'].append({
                    'id': horse_id,
                    'name': horse_name,
                    'error': str(e)
                })
                print(f"    ❌ エラー: {e}")
            
            results['total_processed'] += 1
            
            # 待機（最後以外）
            if i < total:
                time.sleep(delay)
        
        return results
    
    def _print_final_summary(self, results: Dict):
        """最終サマリー表示"""
        list_count = results.get('list_count', 0)
        success_count = len(results.get('success', []))
        failed_count = len(results.get('failed', []))
        total_processed = results.get('total_processed', 0)
        
        print("\n" + "=" * 70)
        print("🏁 完全バッチ処理完了！")
        print("=" * 70)
        print(f"📋 馬リスト取得: {list_count}頭")
        if total_processed > 0:
            print(f"✅ 詳細処理成功: {success_count}頭")
            print(f"❌ 詳細処理失敗: {failed_count}頭")
            print(f"📊 詳細処理成功率: {success_count/total_processed*100:.1f}%")
        print("=" * 70)
    
    def _save_results(self, results: Dict):
        """結果をファイルに保存"""
        os.makedirs('outputs', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"complete_batch_results_{timestamp}.json"
        filepath = os.path.join('outputs', filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"📄 結果保存: {filepath}")
        except Exception as e:
            print(f"❌ ファイル保存エラー: {e}")


# 便利な実行関数

def get_next_100_horses(last_processed_count: int = 0) -> List[Dict]:
    """次の100頭を取得する関数"""
    scraper = SimpleOffsetHorseListScraper()
    
    horses = scraper.get_g1_horses_by_offset(
        offset=last_processed_count,
        max_horses=100,
        min_birth_year=2015
    )
    
    return horses

def run_complete_100_horses_batch():
    """100頭の完全バッチ処理"""
    processor = CompleteBatchProcessor()
    
    results = processor.collect_and_process_horses(
        total_target=100,
        batch_size=25,
        start_offset=0,
        min_birth_year=2015,
        process_details=True,
        delay_between_horses=3
    )
    
    return results

def run_list_only(target: int = 100, offset: int = 0):
    """リスト取得のみ（詳細処理なし）"""
    processor = CompleteBatchProcessor()
    
    results = processor.collect_and_process_horses(
        total_target=target,
        batch_size=25,
        start_offset=offset,
        min_birth_year=2015,
        process_details=False  # 詳細処理なし
    )
    
    return results


# テスト関数
def test_offset_scraper():
    """テスト実行"""
    print("🧪 オフセットスクレイパーテスト")
    
    scraper = SimpleOffsetHorseListScraper()
    horses = scraper.get_g1_horses_by_offset(
        offset=0,
        max_horses=10,
        min_birth_year=2020
    )
    
    print(f"取得結果: {len(horses)}頭")
    
    if horses:
        print("\n📋 取得した馬:")
        for i, horse in enumerate(horses):
            print(f"  {i+1:2d}. {horse['name_ja']} ({horse['birth_year']}年) ID:{horse['id']}")
    
    return horses


if __name__ == "__main__":
    # テスト実行
    test_horses = test_offset_scraper()