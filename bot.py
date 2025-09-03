import os
import json
import asyncio
import websockets
import time
from atproto import Client, models
from dotenv import load_dotenv
import database

load_dotenv()

BLUESKY_HANDLE = os.environ.get('BLUESKY_HANDLE')
BLUESKY_PASSWORD = os.environ.get('BLUESKY_PASSWORD')

VALID_COMMANDS = {'disable', 'enable'}
VALID_CATEGORIES = {'avatar', 'displayname', 'bio', 'handle', 'banner'}

async def detect_profile_changes(user_did, new_record, client=None):
    """Compare new profile record with database state to detect actual changes."""
    changed_categories = set()
    
    # Get previous profile state from database
    previous_profile = await database.get_profile(user_did)
    
    if not previous_profile:
        # First time seeing this user - try to get current API profile for comparison
        if client:
            try:
                current_profile = client.get_profile(actor=user_did)
                # Compare Jetstream new data with current API data
                # Build previous_record from API data for comparison
                previous_record = {}
                if hasattr(current_profile, 'avatar') and current_profile.avatar:
                    previous_record['avatar'] = {'$link': str(current_profile.avatar)}
                if hasattr(current_profile, 'display_name'):
                    previous_record['displayName'] = current_profile.display_name
                if hasattr(current_profile, 'description'):
                    previous_record['description'] = current_profile.description
            except:
                # If API call fails, store new record and return no changes
                await store_profile_from_record(user_did, new_record, client)
                return set()
        else:
            # No previous profile and no client - store new record and return no changes
            await store_profile_from_record(user_did, new_record, client)
            return set()
    else:
        # Convert database record to comparison format
        previous_record = {}
        if previous_profile.get('avatar_ref'):
            previous_record['avatar'] = {'$link': previous_profile['avatar_ref']}
        if previous_profile.get('banner_ref'):
            previous_record['banner'] = {'$link': previous_profile['banner_ref']}
        if previous_profile.get('display_name'):
            previous_record['displayName'] = previous_profile['display_name']
        if previous_profile.get('description'):
            previous_record['description'] = previous_profile['description']
    
    # Compare avatar/profile picture (extract the actual URL for comparison)
    old_avatar_url = ""
    new_avatar_url = ""
    
    # Extract old avatar URL from database format
    if previous_record.get('avatar') and isinstance(previous_record['avatar'], dict):
        old_avatar_url = previous_record['avatar'].get('$link', '')
    
    # Extract new avatar URL from Jetstream format
    if new_record.get('avatar') and isinstance(new_record['avatar'], dict):
        if 'ref' in new_record['avatar'] and isinstance(new_record['avatar']['ref'], dict):
            new_avatar_url = new_record['avatar']['ref'].get('$link', '')
        elif '$link' in new_record['avatar']:
            # Sometimes it might be direct format
            new_avatar_url = new_record['avatar'].get('$link', '')
    
    # Compare avatar URLs for changes
    # (Debug logging removed - avatar comparison working correctly)
    
    if old_avatar_url != new_avatar_url:
        changed_categories.add('avatar')
    
    # Compare banner/header image (extract the actual URL for comparison)
    old_banner_url = ""
    new_banner_url = ""
    
    # Extract old banner URL from database format
    if previous_record.get('banner') and isinstance(previous_record['banner'], dict):
        old_banner_url = previous_record['banner'].get('$link', '')
    
    # Extract new banner URL from Jetstream format
    if new_record.get('banner') and isinstance(new_record['banner'], dict):
        if 'ref' in new_record['banner'] and isinstance(new_record['banner']['ref'], dict):
            new_banner_url = new_record['banner']['ref'].get('$link', '')
        elif '$link' in new_record['banner']:
            # Sometimes it might be direct format
            new_banner_url = new_record['banner'].get('$link', '')
    
    if old_banner_url != new_banner_url:
        changed_categories.add('banner')
    
    # Compare display name
    old_display_name = previous_record.get('displayName', '')
    new_display_name = new_record.get('displayName', '')
    if old_display_name != new_display_name:
        changed_categories.add('displayname')
    
    # Compare bio/description
    old_description = previous_record.get('description', '')
    new_description = new_record.get('description', '')
    if old_description != new_description:
        changed_categories.add('bio')
    
    # Store the updated record in database
    await store_profile_from_record(user_did, new_record, client)
    
    return changed_categories

async def store_profile_from_record(user_did, record, client=None):
    """Helper function to store a profile record in the database."""
    try:
        # Extract avatar reference if it exists
        avatar_ref = None
        if record.get('avatar') and isinstance(record['avatar'], dict):
            avatar_ref = record['avatar'].get('ref', {}).get('$link', '')
        
        # Extract banner reference if it exists  
        banner_ref = None
        if record.get('banner') and isinstance(record['banner'], dict):
            banner_ref = record['banner'].get('ref', {}).get('$link', '')
        
        # Store profile data in database
        
        # Get handle from client if available
        handle = None
        if client:
            try:
                profile = client.get_profile(actor=user_did)
                handle = profile.handle
            except:
                pass
        
        await database.store_profile(
            did=user_did,
            handle=handle or '',
            display_name=record.get('displayName', ''),
            description=record.get('description', ''),
            avatar_ref=avatar_ref or '',
            banner_ref=banner_ref or ''
        )
    except Exception as e:
        print(f"❌ Error storing profile record: {e}")

async def populate_profiles_for_new_follower(client, follower_did):
    """
    Background task to populate profiles for everyone a new follower follows.
    This eliminates 'first time' notifications by preloading profile data.
    """
    try:
        print(f"🔄 Starting profile population for new follower {follower_did}")
        
        # Get who the new follower follows
        following_response = client.get_follows(actor=follower_did)
        following_list = following_response.follows
        
        profiles_to_store = []
        batch_size = 50  # Process in batches to avoid overwhelming the API
        
        for i, followed_user in enumerate(following_list):
            try:
                # Check if we already have this profile in the database
                existing_profile = await database.get_profile(followed_user.did)
                if existing_profile:
                    # Check if handle has changed and update if needed
                    if existing_profile.get('handle') != followed_user.handle:
                        success = await database.update_handle(followed_user.did, followed_user.handle)
                        if success:
                            print(f"🔄 Updated stale handle: {existing_profile.get('handle')} -> @{followed_user.handle}")
                    continue  # Skip further processing if we already have this profile
                
                # Fetch the profile and prepare for batch storage
                profile_data = {
                    'did': followed_user.did,
                    'handle': followed_user.handle,
                    'display_name': followed_user.display_name or '',
                    'description': followed_user.description or '',
                    'avatar_ref': str(followed_user.avatar) if followed_user.avatar else '',
                    'banner_ref': str(followed_user.banner) if followed_user.banner else ''
                }
                profiles_to_store.append(profile_data)
                
                # Store in batches for better performance
                if len(profiles_to_store) >= batch_size:
                    stored_count = await database.batch_store_profiles(profiles_to_store)
                    print(f"📦 Stored batch of {stored_count} profiles (progress: {i+1}/{len(following_list)})")
                    profiles_to_store = []
                    
                    # Small delay to be nice to the API
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                print(f"⚠️  Error processing profile for {followed_user.did}: {e}")
                continue
        
        # Store any remaining profiles
        if profiles_to_store:
            stored_count = await database.batch_store_profiles(profiles_to_store)
            print(f"📦 Stored final batch of {stored_count} profiles")
        
        total_following = len(following_list)
        print(f"✅ Profile population complete for {follower_did}: processed {total_following} following relationships")
        
    except Exception as e:
        print(f"❌ Error during profile population for {follower_did}: {e}")

def trigger_profile_population(client, follower_did):
    """
    Non-blocking trigger for profile population.
    Runs the population job in the background without blocking main operations.
    """
    # Create a background task
    task = asyncio.create_task(populate_profiles_for_new_follower(client, follower_did))
    
    # Add a callback to handle any errors
    def handle_task_result(task_future):
        try:
            task_future.result()
        except Exception as e:
            print(f"❌ Background profile population failed: {e}")
    
    task.add_done_callback(handle_task_result)



def format_change_message(user_profile, changed_categories):
    """Create a specific message about what was changed."""
    display_name = user_profile.display_name or user_profile.handle
    handle = user_profile.handle
    
    if len(changed_categories) == 1:
        change = list(changed_categories)[0]
        change_text = {
            'avatar': 'updated their avatar',
            'banner': 'updated their banner',
            'displayname': 'changed their display name',
            'bio': 'updated their bio',
            'handle': 'changed their handle'
        }.get(change, 'updated their profile')
        
        return f"{display_name} (@{handle}) {change_text}."
    
    elif len(changed_categories) > 1:
        changes = []
        for change in sorted(changed_categories):
            change_text = {
                'avatar': 'avatar',
                'banner': 'banner',
                'displayname': 'display name', 
                'bio': 'bio',
                'handle': 'handle'
            }.get(change, 'profile')
            changes.append(change_text)
        
        if len(changes) == 2:
            changes_text = f"{changes[0]} and {changes[1]}"
        else:
            changes_text = f"{', '.join(changes[:-1])}, and {changes[-1]}"
            
        return f"{display_name} (@{handle}) updated their {changes_text}."
    
    else:
        return f"{display_name} (@{handle}) updated their profile."

def send_dm(client, recipient_did, message_text):
    """Sends a direct message to a user."""
    try:
        dm_client = client.with_bsky_chat_proxy()
        dm = dm_client.chat.bsky.convo

        convo = dm.get_convo_for_members(
            models.ChatBskyConvoGetConvoForMembers.Params(members=[recipient_did]),
        ).convo

        dm.send_message(
            models.ChatBskyConvoSendMessage.Data(
                convo_id=convo.id,
                message=models.ChatBskyConvoDefs.MessageInput(text=message_text),
            )
        )
        print(f"Sent DM to {recipient_did}")
    except Exception as e:
        print(f"Error sending DM to {recipient_did}: {e}")

async def listen_identity_events(client):
    """Connect to Jetstream and listen for handle changes using Phil's fake filter approach."""
    # Phil's recommended approach - fake filter to get identity events
    endpoints = [
        "wss://jetstream1.us-east.fire.hose.cam/subscribe?wantedCollections=nothing.please.thanks&compress=false",
        "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=nothing.please.thanks&compress=false",
        "wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=nothing.please.thanks&compress=false",
        "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=nothing.please.thanks&compress=false"
    ]
    
    for uri in endpoints:
        try:
            print(f"Attempting to connect to: {uri}")
            # Add longer timeout and connection parameters
            async with websockets.connect(
                uri, 
                open_timeout=30,
                close_timeout=10,
                ping_interval=20,
                ping_timeout=10
            ) as websocket:
                print("🚀 Connected to Jetstream! Listening for profile changes, follows, and handle updates...")
                async for message in websocket:
                    try:
                        event = json.loads(message)
                        
                        # Filter events - we only care about follows, profiles, and identity changes
                        event_kind = event.get('kind')
                        commit_collection = event.get('commit', {}).get('collection')
                        
                        # Skip events we don't care about (reduces noise from fake filter)  
                        if event_kind not in ['commit', 'identity']:
                            continue
                            
                        if event_kind == 'commit' and commit_collection not in ['app.bsky.graph.follow', 'app.bsky.actor.profile']:
                            continue
                        
                        # DEBUG: Log what we're actually processing
                        if event_kind == 'commit':
                            print(f"📝 COMMIT EVENT: {commit_collection}")
                        
                        # Check if this is a follow event (someone following the bot)
                        if (event.get('kind') == 'commit' and 
                            event.get('commit', {}).get('collection') == 'app.bsky.graph.follow'):
                            
                            # Check if someone followed our bot
                            follow_record = event.get('commit', {}).get('record', {})
                            if follow_record.get('subject') == client.me.did:
                                follower_did = event.get('did')
                                try:
                                    follower_profile = client.get_profile(actor=follower_did)
                                    print(f"👥 New follower: @{follower_profile.handle}")
                                except:
                                    print(f"👥 New follower: {follower_did}")
                                
                                # Send welcome message
                                welcome_text = """Welcome! I'll send you private notifications when people you follow update their profiles.

I can notify you about:
• Avatar changes
• Banner/header changes
• Display name changes  
• Bio/description updates
• Handle changes

You can customize your notifications by sending me these commands:
• "disable avatar" - Stop avatar change notifications
• "disable banner" - Stop banner change notifications
• "disable displayname" - Stop display name notifications  
• "disable bio" - Stop bio update notifications
• "disable handle" - Stop handle change notifications
• "enable [category]" - Re-enable any disabled notifications

Send just the command (e.g. "disable avatar") and I'll confirm the change!"""
                                send_dm(client, follower_did, welcome_text)
                                print(f"✅ Welcome message sent")
                                
                                # Trigger background profile population for this new follower
                                trigger_profile_population(client, follower_did)
                                print(f"🔄 Started background profile population for {follower_did}")
                        
                        # Check if this is a handle change event (identity events per Phil's advice)
                        elif event.get('kind') == 'identity':
                            changed_did = event.get('did')
                            new_handle = event.get('identity', {}).get('handle')
                            
                            # Skip if we don't have proper data
                            if not changed_did or not new_handle:
                                continue
                                
                            print(f"🏷️  Handle change detected: @{new_handle}")
                            
                            # FIRST: Check if this user has mutual followers (only care about relevant users!)
                            relevant_user = False
                            try:
                                user_followers = client.get_followers(actor=changed_did)
                                bot_followers = client.get_followers(actor=client.me.did)

                                user_follower_dids = {follower.did for follower in user_followers.followers}
                                bot_follower_dids = {follower.did for follower in bot_followers.followers}
                                
                                mutual_followers = user_follower_dids.intersection(bot_follower_dids)
                                
                                if mutual_followers:
                                    relevant_user = True
                                    print(f"🏷️  Handle change (relevant user): @{new_handle}")
                                    
                                    # ONLY NOW update database for relevant users
                                    existing_profile = await database.get_profile(changed_did)
                                    if existing_profile:
                                        # Update just the handle efficiently
                                        success = await database.update_handle(changed_did, new_handle)
                                        if success:
                                            print(f"🔄 Updated handle in database: {changed_did} -> @{new_handle}")
                                        else:
                                            print(f"⚠️  Failed to update handle in database for {changed_did}")
                                    else:
                                        # New relevant user - fetch full profile data instead of empty entry
                                        try:
                                            user_profile = client.get_profile(actor=changed_did)
                                            await database.store_profile(
                                                did=changed_did,
                                                handle=new_handle,
                                                display_name=user_profile.display_name or '',
                                                description=user_profile.description or '',
                                                avatar_ref=str(user_profile.avatar) if user_profile.avatar else '',
                                                banner_ref=str(user_profile.banner) if user_profile.banner else ''
                                            )
                                            print(f"🆕 Created full profile entry for new relevant user: @{new_handle}")
                                        except Exception as fetch_error:
                                            print(f"⚠️  Could not fetch full profile for {changed_did}: {fetch_error}")
                                    
                                    # Send notifications to mutual followers
                                    followers_to_notify = []
                                    for follower_did in mutual_followers:
                                        disabled_cats = await database.get_user_preferences(follower_did)
                                        if 'handle' not in disabled_cats:
                                            followers_to_notify.append(follower_did)

                                    if followers_to_notify:
                                        user_profile = client.get_profile(actor=changed_did)
                                        notification_text = format_change_message(user_profile, {'handle'})
                                        for follower_did in followers_to_notify:
                                            send_dm(client, follower_did, notification_text)
                                        print(f"✅ Sent {len(followers_to_notify)} handle change notification(s)")
                                else:
                                    print(f"🏷️  Handle change (irrelevant user): @{new_handle} - skipped")
                                
                            except Exception as e:
                                print(f"❌ Error processing handle change for {changed_did}: {e}")
                        
                        # Check if this is a profile update
                        elif (event.get('kind') == 'commit' and 
                            event.get('commit', {}).get('collection') == 'app.bsky.actor.profile'):
                            
                            repo_did = event.get('did')
                            operation = event.get('commit', {}).get('operation')
                            
                            # DEBUG: Log profile update events
                            print(f"📝 PROFILE UPDATE EVENT:")
                            print(f"   DID: {repo_did}")
                            print(f"   Operation: {operation}")
                            
                            # Skip "create" operations (new profiles) - only care about "updates"
                            if operation != 'update':
                                print(f"   Skipped (operation = {operation})")
                                continue
                                
                            try:
                                # Check if this user has mutual followers with the bot
                                user_followers = client.get_followers(actor=repo_did)
                                bot_followers = client.get_followers(actor=client.me.did)

                                user_follower_dids = {follower.did for follower in user_followers.followers}
                                bot_follower_dids = {follower.did for follower in bot_followers.followers}
                                
                                mutual_followers = user_follower_dids.intersection(bot_follower_dids)
                                
                                if mutual_followers:
                                    # Someone we care about updated their profile!
                                    user_profile = client.get_profile(actor=repo_did)
                                    
                                    # Get the new record from Jetstream
                                    new_record = event.get('commit', {}).get('record', {})
                                    
                                    # Try to detect what changed (need previous state for this)
                                    changed_categories = await detect_profile_changes(repo_did, new_record, client)
                                    
                                    if changed_categories:
                                        print(f"📝 Profile update: @{user_profile.handle} changed {', '.join(changed_categories)}")
                                        notification_text = format_change_message(user_profile, changed_categories)
                                        
                                        # Filter mutual followers based on their notification preferences
                                        followers_to_notify = []
                                        for follower_did in mutual_followers:
                                            disabled_cats = await database.get_user_preferences(follower_did)
                                            # Check if any of the changed categories are NOT disabled for this user
                                            if not changed_categories.issubset(disabled_cats):
                                                followers_to_notify.append(follower_did)
                                        
                                        # Send notifications to followers who want them
                                        for follower_did in followers_to_notify:
                                            send_dm(client, follower_did, notification_text)
                                        
                                        print(f"✅ Sent {len(followers_to_notify)} notification(s) (filtered by preferences)")
                                    else:
                                        print(f"📝 Profile update: @{user_profile.handle} (unknown changes)")
                                        notification_text = f"{user_profile.display_name or user_profile.handle} (@{user_profile.handle}) updated their profile."
                                        
                                        # For unknown changes, send to all mutual followers
                                        for follower_did in mutual_followers:
                                            send_dm(client, follower_did, notification_text)
                                        
                                        print(f"✅ Sent {len(mutual_followers)} notification(s)")
                                
                            except Exception as e:
                                print(f"❌ Error processing profile update: {e}")
                                
                    except json.JSONDecodeError:
                        print("Failed to parse JSON message")
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"WebSocket connection closed for {uri}: {e}")
            continue  # Try next endpoint
        except websockets.exceptions.WebSocketException as e:
            print(f"WebSocket error for {uri}: {e}")
            continue  # Try next endpoint
        except Exception as e:
            print(f"Error connecting to {uri}: {type(e).__name__}: {e}")
            continue  # Try next endpoint
    
    print("Failed to connect to any Jetstream endpoint")

async def check_followers_and_dms(client):
    """Periodically check for new followers and process DM commands."""
    while True:
        try:
            # Check for new DMs with commands
            try:
                dm_client = client.with_bsky_chat_proxy()
                convos = dm_client.chat.bsky.convo.list_convos().convos
                for convo in convos:
                    if convo.unread_count > 0:
                        messages = dm_client.chat.bsky.convo.get_messages(
                            params=models.ChatBskyConvoGetMessages.Params(convo_id=convo.id)
                        ).messages
                        for msg in messages:
                            # Process incoming messages
                            if msg.sender.did != client.me.did:
                                command_parts = msg.text.lower().split()
                                
                                if len(command_parts) == 2:
                                    command, category = command_parts
                                    user_did = msg.sender.did
                                    
                                    if command in VALID_COMMANDS and category in VALID_CATEGORIES:
                                        if command == 'disable':
                                            success = await database.update_user_preference(user_did, category, True)
                                            reply_text = f"Notifications for {category} changes have been disabled." if success else "Error updating preferences."
                                        else: # enable
                                            success = await database.update_user_preference(user_did, category, False)
                                            reply_text = f"Notifications for {category} changes have been enabled." if success else "Error updating preferences."
                                        
                                        send_dm(client, user_did, reply_text)
                                    else:
                                        reply_text = """Invalid command. Please use one of these formats:

Enable/Disable Commands:
• "disable avatar" - Stop avatar change notifications
• "disable banner" - Stop banner change notifications
• "disable displayname" - Stop display name notifications  
• "disable bio" - Stop bio update notifications
• "disable handle" - Stop handle change notifications
• "enable avatar" - Re-enable avatar notifications
• "enable banner" - Re-enable banner notifications
• "enable displayname" - Re-enable display name notifications
• "enable bio" - Re-enable bio notifications
• "enable handle" - Re-enable handle notifications

Send just the command (e.g. "disable avatar") and I'll confirm the change!"""
                                        send_dm(client, user_did, reply_text)
                                
                                # Mark as read
                                dm_client.chat.bsky.convo.update_read(
                                    models.ChatBskyConvoUpdateRead.Data(convo_id=convo.id)
                                )
            except Exception as e:
                print(f"Error checking DMs: {e}")



        except Exception as e:
            print(f"Error in follower/DM check: {e}")
            
        await asyncio.sleep(10)

async def listen_profile_events(client):
    """Connect to Jetstream and listen for profile updates and follows."""
    # Real Jetstream endpoints with proper filtering for profiles and follows
    endpoints = [
        "wss://jetstream1.us-east.fire.hose.cam/subscribe?wantedCollections=app.bsky.actor.profile,app.bsky.graph.follow&compress=false",
        "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.actor.profile,app.bsky.graph.follow&compress=false", 
        "wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.actor.profile,app.bsky.graph.follow&compress=false",
        "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.actor.profile,app.bsky.graph.follow&compress=false"
    ]
    
    for uri in endpoints:
        try:
            print(f"📝 Connecting to Profile Jetstream: {uri}")
            async with websockets.connect(uri) as websocket:
                print("📝 Connected! Listening for profile updates and follows...")
                async for message in websocket:
                    try:
                        event = json.loads(message)
                        
                        # Only process commit events
                        if event.get('kind') != 'commit':
                            continue
                            
                        commit_collection = event.get('commit', {}).get('collection')
                        
                        # Check if this is a follow event (someone following the bot)
                        if commit_collection == 'app.bsky.graph.follow':
                            commit = event.get('commit', {})
                            operation = commit.get('operation')
                            record = commit.get('record', {})
                            repo_did = event.get('did')
                            
                            # Check if someone is following the bot
                            if operation == 'create' and record.get('subject') == client.me.did:
                                follower_did = repo_did
                                print(f"👤 New follower: {follower_did}")
                                
                                # Send welcome message
                                welcome_message = f"""🤖 Welcome! I'll notify you when people you follow edit their profiles.
                                
Commands:
• disable avatar - stop avatar change notifications
• disable displayname - stop display name change notifications  
• disable bio - stop bio change notifications
• disable handle - stop handle change notifications
• disable banner - stop banner change notifications
• enable [category] - re-enable any category
                                
Just send me a DM with any of these commands!"""
                                
                                send_dm(client, follower_did, welcome_message)
                                print(f"✅ Welcome message sent")
                                
                                # Trigger background profile population for this new follower
                                trigger_profile_population(client, follower_did)
                                print(f"🔄 Started background profile population for {follower_did}")
                        
                        # Check if this is a profile update
                        elif commit_collection == 'app.bsky.actor.profile':
                            repo_did = event.get('did')
                            operation = event.get('commit', {}).get('operation')
                            
                            print(f"📝 PROFILE UPDATE EVENT: {repo_did}, operation: {operation}")
                            
                            # Skip "create" operations (new profiles) - only care about "updates"
                            if operation != 'update':
                                continue
                                
                            try:
                                # Check if this user has mutual followers with the bot
                                user_followers = client.get_followers(actor=repo_did)
                                bot_followers = client.get_followers(actor=client.me.did)

                                user_follower_dids = {follower.did for follower in user_followers.followers}
                                bot_follower_dids = {follower.did for follower in bot_followers.followers}
                                
                                mutual_followers = user_follower_dids.intersection(bot_follower_dids)
                                
                                if mutual_followers:
                                    # Someone we care about updated their profile!
                                    user_profile = client.get_profile(actor=repo_did)
                                    
                                    # Get the new record from Jetstream
                                    new_record = event.get('commit', {}).get('record', {})
                                    
                                    # Try to detect what changed (need previous state for this)
                                    changed_categories = await detect_profile_changes(repo_did, new_record, client)
                                    
                                    if changed_categories:
                                        print(f"📝 Profile changes detected for {user_profile.handle}: {changed_categories}")
                                        
                                        # Filter out users who have disabled specific notifications
                                        followers_to_notify = []
                                        for follower_did in mutual_followers:
                                            disabled_cats = await database.get_user_preferences(follower_did)
                                            # Check if any of the changed categories are NOT disabled
                                            if not changed_categories.issubset(disabled_cats):
                                                # Filter to only include non-disabled categories
                                                enabled_changes = changed_categories - disabled_cats
                                                if enabled_changes:
                                                    followers_to_notify.append((follower_did, enabled_changes))

                                        if followers_to_notify:
                                            for follower_did, enabled_changes in followers_to_notify:
                                                notification_text = format_change_message(user_profile, enabled_changes)
                                                send_dm(client, follower_did, notification_text)
                                            print(f"✅ Sent {len(followers_to_notify)} profile change notification(s)")
                                    else:
                                        print(f"📝 No meaningful changes detected for {user_profile.handle}")
                                        
                            except Exception as e:
                                print(f"❌ Error processing profile update for {repo_did}: {e}")
                        
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"❌ Error processing profile event: {e}")
                        continue
                        
        except Exception as e:
            print(f"❌ Profile listener failed on {uri}: {e}")
            continue
    
    print("❌ All profile endpoints failed.")


async def main():
    """Main function that runs both the Jetstream listener and follower checker."""
    # Initialize database connection
    await database.init_db()
    
    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_PASSWORD)
    print(f"Logged in as {client.me.handle}")

    try:
        # Run THREE tasks concurrently: identity events, profile events, and DM handling
        await asyncio.gather(
            listen_identity_events(client),
            listen_profile_events(client),
            check_followers_and_dms(client)
        )
    finally:
        # Clean up database connection
        await database.close_db()

if __name__ == '__main__':
    asyncio.run(main())