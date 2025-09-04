"""
Database operations for profile edit alerts bot.
"""

import os
import asyncio
import asyncpg
from typing import Optional, Set, Dict, List
from dotenv import load_dotenv
import time

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

# Connection pool - will be initialized on startup
pool = None

async def get_pool():
    """Get the connection pool, creating it if necessary."""
    global pool
    if pool is None:
        try:
            pool = await asyncpg.create_pool(DATABASE_URL)
            print("🗄️  Connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    return pool

async def init_db():
    """Initialize database connection pool and create tables if they don't exist."""
    db_pool = await get_pool()
    if not db_pool:
        print("⚠️  No DATABASE_URL found or connection failed, running in development mode without persistence")
        return None
    
    try:
        await create_tables()
        print("✅ Database tables ready")
        return db_pool
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return None

async def create_tables():
    """Create database tables if they don't exist."""
    db_pool = await get_pool()
    if not db_pool:
        return
        
    async with db_pool.acquire() as conn:
        # Create profiles table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS profiles (
                did TEXT PRIMARY KEY,
                handle TEXT,
                display_name TEXT,
                description TEXT,
                avatar_ref TEXT,
                banner_ref TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Create user preferences table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_did TEXT PRIMARY KEY,
                disabled_avatar BOOLEAN DEFAULT FALSE,
                disabled_banner BOOLEAN DEFAULT FALSE,
                disabled_displayname BOOLEAN DEFAULT FALSE,
                disabled_bio BOOLEAN DEFAULT FALSE,
                disabled_handle BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Create index for faster lookups
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_profiles_did ON profiles(did)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_prefs_did ON user_preferences(user_did)')
        
        # Migration: Add banner column to existing user_preferences table
        try:
            await conn.execute('ALTER TABLE user_preferences ADD COLUMN disabled_banner BOOLEAN DEFAULT FALSE')
            print("🔄 Added disabled_banner column to user_preferences table")
        except Exception:
            # Column already exists, which is fine
            pass

async def execute_query(query, *args, is_fetch=False):
    """Execute a query with reconnection logic."""
    for attempt in range(3): # Try up to 3 times
        try:
            db_pool = await get_pool()
            if not db_pool:
                return None if is_fetch else False
            
            async with db_pool.acquire() as conn:
                if is_fetch:
                    return await conn.fetchrow(query, *args)
                else:
                    return await conn.execute(query, *args)
        except (asyncpg.exceptions.ConnectionDoesNotExistError, OSError) as e:
            print(f"Database connection lost: {e}. Reconnecting... (Attempt {attempt + 1})")
            global pool
            pool = None # Force reconnection
            await asyncio.sleep(2 ** attempt) # Exponential backoff
        except Exception as e:
            print(f"❌ Database query failed: {e}")
            return None if is_fetch else False
    return None if is_fetch else False

async def get_profile(did: str) -> Optional[Dict]:
    """Get a profile from the database."""
    row = await execute_query('SELECT * FROM profiles WHERE did = $1', did, is_fetch=True)
    return dict(row) if row else None

async def store_profile(did: str, handle: str, display_name: str, description: str, 
                       avatar_ref: str, banner_ref: str = None) -> bool:
    """Store or update a profile in the database."""
    query = '''
        INSERT INTO profiles (did, handle, display_name, description, avatar_ref, banner_ref, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (did) 
        DO UPDATE SET 
            handle = EXCLUDED.handle,
            display_name = EXCLUDED.display_name,
            description = EXCLUDED.description,
            avatar_ref = EXCLUDED.avatar_ref,
            banner_ref = EXCLUDED.banner_ref,
            updated_at = NOW()
    '''
    result = await execute_query(query, did, handle, display_name, description, avatar_ref, banner_ref)
    return result is not False

async def batch_store_profiles(profiles: List[Dict]) -> int:
    """Store multiple profiles in a single transaction."""
    db_pool = await get_pool()
    if not db_pool or not profiles:
        return 0
        
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # This is more complex to fit into the execute_query helper due to the transaction.
                # If connection issues are frequent here, this part may also need retry logic.
                for profile in profiles:
                    await conn.execute('''
                        INSERT INTO profiles (did, handle, display_name, description, avatar_ref, banner_ref, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                        ON CONFLICT (did) 
                        DO UPDATE SET 
                            handle = EXCLUDED.handle,
                            display_name = EXCLUDED.display_name,
                            description = EXCLUDED.description,
                            avatar_ref = EXCLUDED.avatar_ref,
                            banner_ref = EXCLUDED.banner_ref,
                            updated_at = NOW()
                    ''', profile['did'], profile['handle'], profile['display_name'], 
                        profile['description'], profile['avatar_ref'], profile.get('banner_ref'))
                return len(profiles)
    except (asyncpg.exceptions.ConnectionDoesNotExistError, OSError) as e:
        print(f"Database connection lost during batch store: {e}. Reconnecting...")
        global pool
        pool = None # Force reconnection
        # Optionally, you could retry the batch operation here.
        return 0
    except Exception as e:
        print(f"❌ Error batch storing profiles: {e}")
        return 0

async def get_user_preferences(user_did: str) -> Set[str]:
    """Get user's disabled notification categories."""
    row = await execute_query('SELECT * FROM user_preferences WHERE user_did = $1', user_did, is_fetch=True)
    if not row:
        return set()
        
    disabled = set()
    if row['disabled_avatar']:
        disabled.add('avatar')
    if row.get('disabled_banner'):
        disabled.add('banner')
    if row['disabled_displayname']:
        disabled.add('displayname')
    if row['disabled_bio']:
        disabled.add('bio')
    if row['disabled_handle']:
        disabled.add('handle')
        
    return disabled

async def update_user_preference(user_did: str, category: str, disabled: bool) -> bool:
    """Update a user's notification preference for a specific category."""
    if category not in ['avatar', 'banner', 'displayname', 'bio', 'handle']:
        return False
        
    # First, ensure the user exists
    insert_query = '''
        INSERT INTO user_preferences (user_did) 
        VALUES ($1) 
        ON CONFLICT (user_did) DO NOTHING
    '''
    await execute_query(insert_query, user_did)
    
    # Then, update the preference
    update_query = f'''
        UPDATE user_preferences 
        SET disabled_{category} = $1, updated_at = NOW()
        WHERE user_did = $2
    '''
    result = await execute_query(update_query, disabled, user_did)
    return result is not False

async def update_handle(did: str, new_handle: str) -> bool:
    """Update just the handle for a specific profile."""
    query = '''
        UPDATE profiles 
        SET handle = $1, updated_at = NOW()
        WHERE did = $2 AND handle != $1
    '''
    result = await execute_query(query, new_handle, did)
    return result and 'UPDATE 1' in result

async def close_db():
    """Close database connection pool."""
    global pool
    if pool:
        await pool.close()
        print("🗄️  Database connection closed")
