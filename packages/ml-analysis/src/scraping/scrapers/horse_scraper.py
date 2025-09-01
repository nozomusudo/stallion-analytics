# src/scraping/scrapers/horse_scraper.py

import sys
import os
from typing import Dict, Optional, Tuple, List

# パスを追加
current_dir = os.path.dirname(os.path.abspath(__file__))
scraping_dir = os.path.dirname(current_dir)
sys.path.append(scraping_dir)

from scrapers.base_scraper import BaseScraper
from extractors.horse.basic_info_extractor import BasicInfoExtractor
from extractors.horse.pedigree_extractor import PedigreeExtractor
from extractors.horse.career_extractor import CareerExtractor
from storage.supabase_storage import SupabaseStorage

class HorseScraper(BaseScraper):
    """馬情報専用スクレイパー"""
    
    def __init__(self, delay: float = 1.0):
        super().__init__(delay)
        self.delay = delay
        self.base_url = "https://db.netkeiba.com"
        # 各抽出クラスを初期化
        self.basic_extractor = BasicInfoExtractor()
        self.pedigree_extractor = PedigreeExtractor()
        self.career_extractor = CareerExtractor()
        
        # ストレージクラス
        self.storage = SupabaseStorage()
    
    def scrape(self, horse_id: str) -> bool:
        """馬の完全な情報を取得・保存"""
        # print(f"\n--- 馬ID: {horse_id} 完全取得開始 ---")
        
        # 1. 基本情報取得
        horse_data = self.scrape_horse_detail(horse_id)
        if not horse_data:
            return False
        
        # 2. 血統関係取得
        relations = self.pedigree_extractor.extract_relations_from_url(horse_id, self.session)
        
        # 3. 血統IDを馬データに追加
        pedigree_ids = self.pedigree_extractor.extract_pedigree_ids_from_relations(relations, horse_id)
        horse_data.update(pedigree_ids)
        
        # 4. 種付関係を作成・更新（children_ids付き）
        mating_relations = self.pedigree_extractor.create_mating_relations(pedigree_ids, horse_id, self.storage)
        relations.extend(mating_relations)
        
        # 5. データ保存
        success = self.storage.save_all(horse_data, relations)
        
        # print(f"{'✅' if success else '❌'} 完了: {horse_data.get('name_ja', 'Unknown')}")
        
        # リクエスト間隔を置く
        self.sleep(1)
        
        return success
    
    def scrape_horse_detail(self, horse_id: str) -> Optional[Tuple[Dict, List[Dict]]]:
        """個別馬の詳細情報を取得"""
        detail_url = f"{self.base_url}/horse/{horse_id}/"
        
        soup = self.get_soup(detail_url)
        if not soup:
            return None
        
        try:
            # print(f"🔍 馬詳細ページ取得: {horse_id}")
            
            # 基本情報を抽出
            horse_data = self.basic_extractor.extract(soup, horse_id)
            
            # 血統IDを直接取得
            pedigree_ids = self.pedigree_extractor.extract_pedigree_ids(soup)
            horse_data.update(pedigree_ids)
            
            # 主な勝ち鞍を抽出
            victories = self.career_extractor.extract_main_victories(soup)
            if victories:
                horse_data['main_victories'] = victories
                # print(f"    🏆 勝ち鞍取得: {len(victories)}レース")
            
            # 通算成績を抽出
            career_record = self.career_extractor.extract_career_record(soup)
            if career_record:
                horse_data['career_record'] = career_record
                # print(f"    📊 成績取得: {career_record}")
            
            # 血統関係を抽出
            relations = self.pedigree_extractor.extract_relations_from_url(horse_id, self.session)
            pedigree_ids = self.pedigree_extractor.extract_pedigree_ids_from_relations(relations, horse_id)
            horse_data.update(pedigree_ids)

            # 4. 種付関係を作成・更新（children_ids付き）
            mating_relations = self.pedigree_extractor.create_mating_relations(pedigree_ids, horse_id, self.storage)
            relations.extend(mating_relations)

            # Profile情報を整理
            # 直接カラム用のデータを分離
            direct_columns = {
                'id': horse_data['id'],
                'name_ja': horse_data.get('name_ja'),
                'name_en': horse_data.get('name_en'),
                'birth_date': horse_data.get('birth_date'),
                'sex': horse_data.get('sex'),
                'sire_id': horse_data.get('sire_id'),
                'dam_id': horse_data.get('dam_id'),
                'maternal_grandsire_id': horse_data.get('maternal_grandsire_id')
            }
            
            # profile用のデータ（直接カラム以外）
            profile_data = {
                key: value for key, value in horse_data.items() 
                if key not in ['id', 'name_ja', 'name_en', 'birth_date', 'sex', 'sire_id', 'dam_id', 'maternal_grandsire_id']
            }
            
            # 最終的な保存データ
            save_data = {**direct_columns, 'profile': profile_data}
            
            # print(f"    📋 取得データ項目数: {len(horse_data)}")
            # print(f"  ✅ 基本情報取得成功: {horse_data.get('name_ja', 'Unknown')}")
            
            return save_data, relations
            
        except Exception as e:
            print(f"  ❌ 詳細取得エラー (馬ID: {horse_id}): {e}")
            return None

# 使用例
if __name__ == "__main__":
    scraper = HorseScraper()
    
    # テスト実行（イクイノックス）
    test_horse_id = "2019105219"
    success = scraper.scrape(test_horse_id)
    
    if success:
        print("🎉 詳細情報取得完了！")
    else:
        print("❌ 取得失敗")