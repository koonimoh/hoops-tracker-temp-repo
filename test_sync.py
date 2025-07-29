# test_sync.py

import sys
import logging
from dotenv import load_dotenv
from supabase_client import SupabaseClient
from nba_service import NBAService

def main(sync_method_name: str, table_name: str):
    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    supabase = SupabaseClient()
    nba      = NBAService()
    nba.set_supabase_client(supabase)

    # 1. Run the requested sync method
    sync_method = getattr(nba, sync_method_name, None)
    if not sync_method:
        print(f"❌ No method nba_service.{sync_method_name}()")
        sys.exit(1)

    logging.info(f"🔄 Starting {sync_method_name}()...")
    result = sync_method()
    logging.info(f"✅ {sync_method_name}() returned: {result}")

    # 2. Query Supabase for the exact count of rows in your chosen table
    resp = (
        supabase.client
            .schema("hoops")
            .from_(table_name)
            .select("id", count="exact")
            .execute()
    )
    count = resp.count or 0
    logging.info(f"🎯 hoops.{table_name} now contains exactly {count} rows")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test_sync.py <sync_method> <table_name>")
        print("Example: python test_sync.py sync_players players")
        sys.exit(1)

    _, sync_method, table = sys.argv
    main(sync_method, table)
