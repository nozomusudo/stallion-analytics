"""
競馬データ一括取得スクリプト
HorseListScraperとHorseScraperを組み合わせて効率的にデータ収集
"""

import sys
import os
import time
from typing import List, Dict, Optional
from datetime import datetime
import json

# パス設定（Jupyter用）
current_dir = os.getcwd()
src_path = os.path.join(current_dir, 'src', 'scraping')
if src_path not in sys.path:
    sys.path.append(src_path)

# 自作モジュールをインポート
from horse_list_scraper import HorseListScraper
from scrapers.horse_scraper import HorseScraper


class HorseBatchScraper:
    """馬データの一括取得・保存クラス"""
    
    def __init__(self):
        self.list_scraper = HorseListScraper()
        self.detail_scraper = HorseScraper()
        self.results = {
            'success': [],
            'failed': [],
            'skipped': [],
            'total_processed': 0
        }
    
    def run_batch_collection(
        self, 
        max_horses: int = 10,
        min_birth_year: int = 2018,
        skip_existing: bool = True,
        delay_between_horses: int = 2
    ) -> Dict:
        """
        馬データの一括収集実行
        
        Args:
            max_horses: 処理する最大馬数
            min_birth_year: 対象最小生年
            skip_existing: 既存データをスキップするか
            delay_between_horses: 馬ごとの処理間隔（秒）
        
        Returns:
            処理結果の辞書
        """
        print("=" * 60)
        print("🏇 競馬データ一括収集開始")
        print("=" * 60)
        print(f"📋 設定:")
        print(f"   - 対象馬数: {max_horses}頭")
        print(f"   - 対象生年: {min_birth_year}年以降")
        print(f"   - 既存スキップ: {'有効' if skip_existing else '無効'}")
        print(f"   - 処理間隔: {delay_between_horses}秒")
        print()
        
        # ステップ1: G1馬リスト取得
        print("🔍 ステップ1: G1馬リスト取得")
        horse_list = self.list_scraper.scrape_g1_horses(
            max_horses=max_horses, 
            min_birth_year=min_birth_year
        )
        
        if not horse_list:
            print("❌ G1馬リストの取得に失敗しました")
            return self.results
        
        print(f"✅ {len(horse_list)}頭の候補馬を取得")
        print()
        
        # ステップ2: 既存データのチェック（オプション）
        if skip_existing:
            horse_list = self._filter_existing_horses(horse_list)
            print(f"📋 スキップ後: {len(horse_list)}頭が処理対象")
            print()
        
        # ステップ3: 詳細データ取得
        print("🔍 ステップ3: 各馬の詳細データ取得")
        self._process_horses_details(horse_list, delay_between_horses)
        
        # 結果サマリー
        self._print_results_summary()
        
        return self.results
    
    def _filter_existing_horses(self, horse_list: List[Dict]) -> List[Dict]:
        """既存データをフィルターして重複を避ける"""
        if not self.list_scraper.supabase:
            print("⚠️ Supabaseが利用できないため、重複チェックをスキップします")
            return horse_list
        
        try:
            # 既存の馬IDを取得
            existing_response = self.list_scraper.supabase.table('horses').select('id').execute()
            existing_ids = {row['id'] for row in existing_response.data}
            
            # 新しい馬のみをフィルター
            new_horses = [horse for horse in horse_list if horse['id'] not in existing_ids]
            
            skipped_count = len(horse_list) - len(new_horses)
            if skipped_count > 0:
                print(f"📋 {skipped_count}頭は既にデータベースに存在するためスキップ")
                self.results['skipped'] = [
                    horse for horse in horse_list if horse['id'] in existing_ids
                ]
            
            return new_horses
            
        except Exception as e:
            print(f"⚠️ 既存データチェックでエラー: {e}")
            print("   すべての馬を処理対象とします")
            return horse_list
    
    def _process_horses_details(self, horse_list: List[Dict], delay: int):
        """各馬の詳細データを順次取得"""
        total = len(horse_list)
        
        for i, horse in enumerate(horse_list, 1):
            horse_id = str(horse['id'])
            horse_name = horse['name_ja']
            
            print(f"🐎 [{i}/{total}] {horse_name} (ID: {horse_id}) 処理開始...")
            
            try:
                # 詳細データ取得
                success = self.detail_scraper.scrape(horse_id)
                
                if success:
                    self.results['success'].append({
                        'id': horse_id,
                        'name': horse_name,
                        'processed_at': datetime.now().isoformat()
                    })
                    print(f"   ✅ 完了")
                else:
                    self.results['failed'].append({
                        'id': horse_id,
                        'name': horse_name,
                        'error': 'スクレイピング失敗'
                    })
                    print(f"   ❌ 失敗")
                
            except Exception as e:
                self.results['failed'].append({
                    'id': horse_id,
                    'name': horse_name,
                    'error': str(e)
                })
                print(f"   ❌ エラー: {e}")
            
            self.results['total_processed'] += 1
            
            # 最後以外は待機
            if i < total:
                print(f"   ⏳ {delay}秒待機中...")
                time.sleep(delay)
            
            print()
    
    def _print_results_summary(self):
        """処理結果のサマリーを表示"""
        success_count = len(self.results['success'])
        failed_count = len(self.results['failed'])
        skipped_count = len(self.results['skipped'])
        total = self.results['total_processed']
        
        print("=" * 60)
        print("📊 処理結果サマリー")
        print("=" * 60)
        print(f"✅ 成功: {success_count}頭")
        print(f"❌ 失敗: {failed_count}頭") 
        print(f"⏭️ スキップ: {skipped_count}頭")
        print(f"📋 処理済総数: {total}頭")
        
        if success_count > 0:
            print(f"\n🎉 成功率: {success_count/total*100:.1f}%")
            print("\n✅ 成功した馬:")
            for horse in self.results['success']:
                print(f"   - {horse['name']} (ID: {horse['id']})")
        
        if failed_count > 0:
            print(f"\n❌ 失敗した馬:")
            for horse in self.results['failed']:
                print(f"   - {horse['name']} (ID: {horse['id']}): {horse['error']}")
        
        print("=" * 60)
    
    def save_results_log(self, filename: Optional[str] = None):
        """処理結果をJSONファイルに保存"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_scraping_results_{timestamp}.json"
        
        filepath = os.path.join('outputs', filename)
        os.makedirs('outputs', exist_ok=True)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
            print(f"📄 結果ログを保存: {filepath}")
        except Exception as e:
            print(f"❌ ログ保存エラー: {e}")


# Jupyter Notebook用の便利関数
def quick_batch_scrape(
    max_horses: int = 10,
    min_birth_year: int = 2020,
    delay: int = 2,
    save_log: bool = True
) -> Dict:
    """
    クイック実行用関数（Jupyter Notebook向け）
    
    Args:
        max_horses: 取得する馬数
        min_birth_year: 最小生年
        delay: 処理間隔（秒）
        save_log: 結果をファイルに保存するか
    
    Returns:
        処理結果
    """
    scraper = HorseBatchScraper()
    results = scraper.run_batch_collection(
        max_horses=max_horses,
        min_birth_year=min_birth_year,
        delay_between_horses=delay
    )
    
    if save_log:
        scraper.save_results_log()
    
    return results


# 使用例
if __name__ == "__main__":
    # 10頭でテスト実行
    batch_scraper = HorseBatchScraper()
    results = batch_scraper.run_batch_collection(
        max_horses=10,
        min_birth_year=2020,
        delay_between_horses=2
    )
    
    # 結果をファイルに保存
    batch_scraper.save_results_log()