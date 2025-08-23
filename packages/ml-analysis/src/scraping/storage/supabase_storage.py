# src/scraping/storage/supabase_storage.py

import os
from typing import Dict, List
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

class SupabaseStorage:
    """Supabaseへのデータ保存を管理するクラス"""
    
    def __init__(self):
        supabase_url = os.getenv('NEXT_PUBLIC_SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if supabase_url and supabase_key:
            self.supabase: Client = create_client(supabase_url, supabase_key)
        else:
            print("⚠️ Supabase環境変数が設定されていません")
            self.supabase = None
    
    def save_horse_data(self, horse_data: Dict) -> bool:
        """馬の基本情報をSupabaseに保存"""
        if not self.supabase:
            return False
        
        try:
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
            
            # None値を除去
            save_data = {k: v for k, v in save_data.items() if v is not None}
            
            # upsertで保存
            result = self.supabase.table('horses').upsert(
                save_data, on_conflict='id'
            ).execute()
            
            print(f"    💾 保存データ構造:")
            print(f"      直接カラム: {list(direct_columns.keys())}")
            print(f"      プロフィール項目: {list(profile_data.keys())}")
            
            return True
            
        except Exception as e:
            print(f"    ❌ 馬データ保存エラー: {e}")
            return False
    
    def save_relations(self, relations: List[Dict]) -> bool:
        """血統関係をSupabaseに保存"""
        if not self.supabase or not relations:
            return False
        
        try:
            saved_count = 0
            
            for relation in relations:
                # 重複チェック
                existing = self.supabase.table('horse_relations').select('id').eq(
                    'horse_a_id', relation['horse_a_id']
                ).eq('horse_b_id', relation['horse_b_id']).eq(
                    'relation_type', relation['relation_type']
                ).execute()
                
                if not existing.data:
                    clean_relation = {
                        'horse_a_id': relation['horse_a_id'],
                        'horse_b_id': relation['horse_b_id'],
                        'relation_type': relation['relation_type'],
                        'children_ids': relation.get('children_ids')
                    }
                    
                    self.supabase.table('horse_relations').insert(clean_relation).execute()
                    saved_count += 1
                    print(f"      💾 関係保存: {relation['relation_type']} ({relation['horse_a_id']} -> {relation['horse_b_id']})")
                else:
                    print(f"      💡 関係既存: {relation['relation_type']}")
            
            print(f"    ✅ {saved_count}件の関係を保存")
            return True
            
        except Exception as e:
            print(f"    ❌ 関係保存エラー: {e}")
            return False
    
    def save_all(self, horse_data: Dict, relations: List[Dict]) -> bool:
        """馬データと関係データを一括保存"""
        horse_saved = self.save_horse_data(horse_data)
        relations_saved = self.save_relations(relations) if relations else True
        
        return horse_saved and relations_saved