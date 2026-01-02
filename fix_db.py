# fix_db.py - Soddalashtirilgan bot uchun database tuzatish
import aiosqlite
import asyncio

async def fix_existing_db():
    """
    Eski bazani yangi strukturaga moslashtiradi.
    Eski AI va Liga jadvallarini o'chiradi, yangilarini qo'shadi.
    """
    async with aiosqlite.connect("bot_data.db") as db:
        print("🔧 Database tuzatish boshlandi...")
        
        # === 1. USERS JADVALI ===
        print("📋 Users jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            almaz       INTEGER DEFAULT 0,
            ref_by      INTEGER,
            verified    INTEGER DEFAULT 0,
            phone       TEXT,
            created_at  INTEGER
        );
        """)
        
        # Eski rank ustunlarini o'chirish (agar mavjud bo'lsa)
        try:
            # Avval jadval strukturasini tekshiramiz
            cursor = await db.execute("PRAGMA table_info(users)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Agar rank ustunlari mavjud bo'lsa, ularni o'chiramiz
            if "rank_score" in column_names or "rank_level" in column_names:
                print("🗑️ Eski rank ustunlarini o'chirish...")
                # SQLite da ALTER TABLE DROP COLUMN yo'q, shuning uchun yangi jadval yaratamiz
                await db.execute("""
                CREATE TABLE users_new (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    almaz       INTEGER DEFAULT 0,
                    ref_by      INTEGER,
                    verified    INTEGER DEFAULT 0,
                    phone       TEXT,
                    created_at  INTEGER
                );
                """)
                
                # Ma'lumotlarni ko'chiramiz
                await db.execute("""
                INSERT INTO users_new (user_id, username, almaz, ref_by, verified, phone, created_at)
                SELECT user_id, username, almaz, ref_by, verified, phone, created_at FROM users;
                """)
                
                # Eski jadvalni o'chirib, yangisini o'rniga qo'yamiz
                await db.execute("DROP TABLE users;")
                await db.execute("ALTER TABLE users_new RENAME TO users;")
                print("✅ Rank ustunlari o'chirildi")
        except Exception as e:
            print(f"⚠️ Users jadvali yangilashda xato (normal bo'lishi mumkin): {e}")
        
        # === 2. REQUIRED CHANNELS ===
        print("📋 Required channels jadvalini tuzatish...")
        await db.execute("DROP TABLE IF EXISTS required_channels;")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS required_channels (
            username TEXT PRIMARY KEY
        );
        """)
        
        # === 3. ADMINS JADVALI ===
        print("📋 Admins jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id  INTEGER PRIMARY KEY,
            username TEXT
        );
        """)
        
        # === 4. DYNAMIC TEXTS ===
        print("📋 Dynamic texts jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS dynamic_texts (
            key     TEXT PRIMARY KEY,
            content TEXT
        );
        """)
        
        # === 5. SUSPENSIONS ===
        print("📋 Suspensions jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS suspensions (
            user_id  INTEGER PRIMARY KEY,
            until_ts INTEGER
        );
        """)
        
        # === 6. REFERRALS ===
        print("📋 Referrals jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            inviter_id  INTEGER NOT NULL,
            invited_id  INTEGER NOT NULL UNIQUE,
            status      TEXT DEFAULT 'joined',
            created_at  INTEGER,
            verified_at INTEGER
        );
        """)
        
        # === 7. WITHDRAW REQUESTS ===
        print("📋 Withdraw requests jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            amount       INTEGER NOT NULL,
            ff_id        TEXT,
            status       TEXT DEFAULT 'pending',
            created_at   INTEGER,
            processed_at INTEGER,
            processed_by INTEGER,
            note         TEXT
        );
        """)
        
        # === 8. WITHDRAW NOTIFICATIONS ===
        print("📋 Withdraw notifications jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_notifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            chat_id    INTEGER NOT NULL,
            message_id INTEGER NOT NULL
        );
        """)
        
        # === 9. SETTINGS ===
        print("📋 Settings jadvalini tekshirish...")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        """)
        
        # === 10. KERAKSIZ JADVALLARNI O'CHIRISH ===
        print("🗑️ Keraksiz jadvallarni o'chirish...")
        
        # Groups jadvali (soddalashtirilgan versiyada kerak emas)
        await db.execute("DROP TABLE IF EXISTS groups;")
        print("   ✓ Groups jadvali o'chirildi")
        
        # AI limits jadvali (AI funksiyalari olib tashlangan)
        await db.execute("DROP TABLE IF EXISTS ai_limits;")
        print("   ✓ AI limits jadvali o'chirildi")
        
        await db.commit()
        print("\n✅ Barcha jadvallar muvaffaqiyatli tuzatildi!")
        print("\n📊 Joriy jadvallar:")
        print("   • users")
        print("   • admins")
        print("   • required_channels")
        print("   • dynamic_texts")
        print("   • suspensions")
        print("   • referrals")
        print("   • withdraw_requests")
        print("   • withdraw_notifications")
        print("   • settings")
        print("\n🎉 Database tayyor!")


async def check_database():
    """
    Bazadagi jadvallar ro'yxatini ko'rsatadi
    """
    async with aiosqlite.connect("bot_data.db") as db:
        cursor = await db.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name;
        """)
        tables = await cursor.fetchall()
        
        print("\n📊 Bazadagi barcha jadvallar:")
        for table in tables:
            print(f"   • {table[0]}")
            
            # Har bir jadval uchun qatorlar sonini ko'rsatish
            count_cursor = await db.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = await count_cursor.fetchone()
            print(f"     └─ {count[0]} ta yozuv")


if __name__ == "__main__":
    print("="*50)
    print("🔧 FreeFire Bot - Database Tuzatish Dasturi")
    print("="*50)
    print()
    
    # Bazani tuzatish
    asyncio.run(fix_existing_db())
    
    # Natijani tekshirish
    print()
    asyncio.run(check_database())