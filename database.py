"""
Database operations for profile edit alerts bot.
"""

import os
import asyncio
import asyncpg
from typing import Optional, Set, Dict, List
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

# Connection pool - will be initialized on startup
pool = None

async def init_db():
    """Initialize database connection pool and create tables if they don't exist."""
    global pool
    
    if not DATABASE_URL:
        print("⚠️  No DATABASE_URL found, running in development mode without persistence")
        return None
    
    try:
        # Create connection pool
        pool = await asyncpg.create_pool(DATABASE_URL)
        print("🗄️  Connected to PostgreSQL database")
        
        # Create tables
        await create_tables()
        print("✅ Database tables ready")
        
        return pool
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

async def create_tables():
    """Create database tables if they don't exist."""
    if not pool:
        return
        
    async with pool.acquire() as conn:
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

async def get_profile(did: str) -> Optional[Dict]:
    """Get a profile from the database."""
    if not pool:
        return None
        
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM profiles WHERE did = $1', did)
        return dict(row) if row else None

async def store_profile(did: str, handle: str, display_name: str, description: str, 
                       avatar_ref: str, banner_ref: str = None) -> bool:
    """Store or update a profile in the database."""
    if not pool:
        return False
        
    try:
        async with pool.acquire() as conn:
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
            ''', did, handle, display_name, description, avatar_ref, banner_ref)
        return True
    except Exception as e:
        print(f"❌ Error storing profile {did}: {e}")
        return False

async def batch_store_profiles(profiles: List[Dict]) -> int:
    """Store multiple profiles in a single transaction."""
    if not pool or not profiles:
        return 0
        
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                count = 0
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
                    count += 1
                return count
    except Exception as e:
        print(f"❌ Error batch storing profiles: {e}")
        return 0

async def get_user_preferences(user_did: str) -> Set[str]:
    """Get user's disabled notification categories."""
    if not pool:
        return set()
        
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM user_preferences WHERE user_did = $1', user_did)
        if not row:
            return set()
            
        disabled = set()
        if row['disabled_avatar']:
            disabled.add('avatar')
        if row.get('disabled_banner'):  # Use .get() in case column doesn't exist yet
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
    if not pool:
        return False
        
    if category not in ['avatar', 'banner', 'displayname', 'bio', 'handle']:
        return False
        
    try:
        async with pool.acquire() as conn:
            # First, ensure the user exists in the table
            await conn.execute('''
                INSERT INTO user_preferences (user_did) 
                VALUES ($1) 
                ON CONFLICT (user_did) DO NOTHING
            ''', user_did)
            
            # Update the specific preference
            column_name = f'disabled_{category}'
            await conn.execute(f'''
                UPDATE user_preferences 
                SET {column_name} = $1, updated_at = NOW()
                WHERE user_did = $2
            ''', disabled, user_did)
        return True
    except Exception as e:
        print(f"❌ Error updating preference for {user_did}: {e}")
        return False

async def update_handle(did: str, new_handle: str) -> bool:
    """Update just the handle for a specific profile."""
    if not pool:
        return False
        
    try:
        async with pool.acquire() as conn:
            result = await conn.execute('''
                UPDATE profiles 
                SET handle = $1, updated_at = NOW()
                WHERE did = $2
            ''', new_handle, did)
            return result == "UPDATE 1"
    except Exception as e:
        print(f"❌ Error updating handle for {did}: {e}")
        return False

async def close_db():
    """Close database connection pool."""
    global pool
    if pool:
        await pool.close()
        print("🗄️  Database connection closed")
