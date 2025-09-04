import os
import json
import asyncio
import websockets
from dotenv import load_dotenv
from atproto import Client, models
import database

# Load environment variables
load_dotenv()

VALID_CATEGORIES = ['avatar', 'displayname', 'bio', 'banner', 'handle']

async def detect_profile_changes(user_did, new_record, client=None):
    """Compare new profile record with stored profile to detect changes."""
    changed_categories = []
    
    try:
        # Get the previous profile from database
        previous_profile = await database.get_profile(user_did)
        
        # If no previous profile, we can't detect changes, but we will store it for next time.
        if not previous_profile:
            pass # Explicitly pass here, we'll store at the end

        if previous_profile:
            # Compare display name
            new_display_name = new_record.get('displayName', '')
            if new_display_name != previous_profile.get('display_name', ''):
                changed_categories.append('displayname')
            
            # Compare description/bio
            new_description = new_record.get('description', '')
            if new_description != previous_profile.get('description', ''):
                changed_categories.append('bio')
            
            # Compare avatar - extract the actual URL string for comparison
            new_avatar_ref = ''
            if 'avatar' in new_record and new_record['avatar']:
                if isinstance(new_record['avatar'], dict) and '$link' in new_record['avatar']:
                    new_avatar_ref = new_record['avatar']['$link']
                else:
                    new_avatar_ref = str(new_record['avatar'])
            
            if new_avatar_ref != previous_profile.get('avatar_ref', ''):
                changed_categories.append('avatar')
            
            # Compare banner - extract the actual URL string for comparison
            new_banner_ref = ''
            if 'banner' in new_record and new_record['banner']:
                if isinstance(new_record['banner'], dict) and '$link' in new_record['banner']:
                    new_banner_ref = new_record['banner']['$link']
                else:
                    new_banner_ref = str(new_record['banner'])
            
            if new_banner_ref != previous_profile.get('banner_ref', ''):
                changed_categories.append('banner')
        
        # Store the updated profile in database
        store_task = asyncio.create_task(store_profile_from_record(user_did, new_record, client))
        # Optional: await if you need to ensure it's stored before returning
        # await store_task 
        
        return changed_categories
        
    except Exception as e:
        print(f"Error detecting profile changes for {user_did}: {e}")
        return []

async def store_profile_from_record(user_did, record, client=None):
    """Extract profile fields from record and store in database."""
    try:
        # Get handle - try from record first, then from client
        handle = record.get('handle', '')
        if not handle and client:
            try:
                profile = client.get_profile(actor=user_did)
                handle = profile.handle
            except:
                pass
        
        # Extract avatar reference
        avatar_ref = ''
        if 'avatar' in record and record['avatar']:
            if isinstance(record['avatar'], dict) and '$link' in record['avatar']:
                avatar_ref = record['avatar']['$link']
            else:
                avatar_ref = str(record['avatar'])
        
        # Extract banner reference
        banner_ref = ''
        if 'banner' in record and record['banner']:
            if isinstance(record['banner'], dict) and '$link' in record['banner']:
                banner_ref = record['banner']['$link']
            else:
                banner_ref = str(record['banner'])
        
        await database.store_profile(
            did=user_did,
            handle=handle,
            display_name=record.get('displayName', ''),
            description=record.get('description', ''),
            avatar_ref=avatar_ref,
            banner_ref=banner_ref
        )
        
    except Exception as e:
        print(f"Error storing profile for {user_did}: {e}")

async def populate_profiles_for_new_follower(client, follower_did):
    """Background task to populate profiles for people the new follower follows."""
    try:
        print(f"🔄 Starting profile population for {follower_did}")
        
        # Get the list of people this follower follows
        following = client.get_follows(actor=follower_did)
        
        profiles_to_store = []
        for follow in following.follows:
            try:
                profile = client.get_profile(actor=follow.did)
                
                # Check if handle has changed since last stored (if any)
                existing_profile = await database.get_profile(follow.did)
                if existing_profile and existing_profile.get('handle') != profile.handle:
                    print(f"🔄 Detected stale handle during population: {existing_profile.get('handle')} -> {profile.handle}")
                    await database.update_handle(follow.did, profile.handle)
                
                profiles_to_store.append({
                    'did': follow.did,
                    'handle': profile.handle,
                    'display_name': profile.display_name or '',
                    'description': profile.description or '',
                    'avatar_ref': str(profile.avatar) if profile.avatar else '',
                    'banner_ref': str(profile.banner) if profile.banner else ''
                })
                
                # Process in batches to avoid overwhelming the database
                if len(profiles_to_store) >= 50:
                    await database.batch_store_profiles(profiles_to_store)
                    profiles_to_store = []
                    print(f"📦 Batch stored 50 profiles")
                    
            except Exception as e:
                print(f"Error fetching profile {follow.did}: {e}")
                continue
        
        # Store remaining profiles
        if profiles_to_store:
            await database.batch_store_profiles(profiles_to_store)
            print(f"📦 Final batch stored {len(profiles_to_store)} profiles")
        
        print(f"✅ Profile population completed for {follower_did}")
        
    except Exception as e:
        print(f"Error in profile population for {follower_did}: {e}")

def trigger_profile_population(client, follower_did):
    """Trigger background profile population task."""
    asyncio.create_task(populate_profiles_for_new_follower(client, follower_did))

def format_change_message(user_profile, changed_categories):
    """Format a notification message based on changed categories."""
    category_messages = {
        'avatar': 'updated their avatar',
        'banner': 'updated their banner',
        'displayname': 'changed their display name',
        'bio': 'updated their bio',
        'handle': 'changed their handle'
    }
    
    # Use handle and include display name for context
    handle = user_profile.handle
    display_name = user_profile.display_name
    user_identifier = f"@{handle}"
    if display_name:
        user_identifier += f" ({display_name})"
    
    if len(changed_categories) == 1:
        action = category_messages[changed_categories[0]]
        return f"📝 Profile Update\n\n{user_identifier} {action}"
    else:
        actions = [category_messages[cat] for cat in changed_categories]
        if len(actions) == 2:
            action_text = f"{actions[0]} and {actions[1]}"
        else:
            action_text = f"{', '.join(actions[:-1])}, and {actions[-1]}"
        return f"📝 Profile Update\n\n{user_identifier} {action_text}"

def send_dm(client, recipient_did, message_text):
    """Send a direct message to a user."""
    try:
        dm_client = client.with_bsky_chat_proxy()
        dm = dm_client.chat.bsky.convo
        
        convo = dm.get_convo_for_members(
            models.ChatBskyConvoGetConvoForMembers.Params(members=[recipient_did])
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

async def process_identity_event(client, event):
    """Process a single identity event asynchronously."""
    changed_did = event.get('did')
    new_handle = event.get('identity', {}).get('handle')
    
    # Skip if we don't have proper data
    if not changed_did or not new_handle:
        return
        
    # FIRST: Check if this user has mutual followers (only care about relevant users!)
    try:
        user_followers = client.get_followers(actor=changed_did)
        bot_followers = client.get_followers(actor=client.me.did)

        user_follower_dids = {follower.did for follower in user_followers.followers}
        bot_follower_dids = {follower.did for follower in bot_followers.followers}
        
        mutual_followers = user_follower_dids.intersection(bot_follower_dids)
        
        if mutual_followers:
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
                    print(f"🆕 Added new relevant user to database: @{new_handle}")
                except Exception as e:
                    print(f"❌ Error fetching profile for new relevant user {changed_did}: {e}")
                    return
            
            # Send DMs to mutual followers about the handle change
            old_handle = existing_profile.get('handle') if existing_profile else 'Someone'
            old_display_name = existing_profile.get('display_name') if existing_profile else ''
            
            user_identifier = f"@{old_handle}"
            if old_display_name:
                user_identifier += f" ({old_display_name})"

            message = f"🏷️ Handle Update\n\n{user_identifier} you follow changed their handle to @{new_handle}"
            
            for follower_did in mutual_followers:
                try:
                    user_prefs = await database.get_user_preferences(follower_did)
                    if 'handle' not in user_prefs:
                        send_dm(client, follower_did, message)
                        print(f"📬 Sent handle change notification to {follower_did}")
                except Exception as e:
                    print(f"❌ Error sending handle change notification to {follower_did}: {e}")
        # Silently skip irrelevant users (no need to log spam)
    except Exception as e:
        # Handle "Actor not found" errors gracefully (deleted/suspended accounts)
        error_str = str(e)
        if "Actor not found" in error_str or "InvalidRequest" in error_str:
            print(f"ℹ️  Handle change for deleted/suspended account: @{new_handle} (DID: {changed_did}) - skipped")
        else:
            print(f"❌ Error processing handle change for {changed_did}: {e}")

async def listen_all_events(client):
    """Connect to Jetstream and listen for all relevant events."""
    endpoints = [
        "wss://jetstream1.us-east.fire.hose.cam/subscribe",
        "wss://jetstream2.us-west.bsky.network/subscribe",
        "wss://jetstream1.us-west.bsky.network/subscribe",
        "wss://jetstream1.us-east.bsky.network/subscribe"
    ]
    
    for uri in endpoints:
        try:
            print(f"🔗 Connecting to Jetstream: {uri}")
            async with websockets.connect(
                uri, 
                open_timeout=30,
                close_timeout=10,
                ping_interval=20,
                ping_timeout=10
            ) as websocket:
                print("✅ Connected! Listening for all events...")
                async for message in websocket:
                    try:
                        event = json.loads(message)
                        
                        if event.get('kind') == 'identity':
                            asyncio.create_task(process_identity_event(client, event))
                        
                        elif event.get('kind') == 'commit':
                            commit_collection = event.get('commit', {}).get('collection')
                            
                            if commit_collection == 'app.bsky.graph.follow':
                                asyncio.create_task(process_follow_event(client, event))
                                
                            elif commit_collection == 'app.bsky.actor.profile':
                                asyncio.create_task(process_profile_event(client, event))
                        
                        await asyncio.sleep(0)
                                
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"❌ Error processing event: {e}")
                        await asyncio.sleep(0)
                        continue
                        
        except Exception as e:
            print(f"❌ Jetstream connection failed to {uri}: {e}")
            await asyncio.sleep(5)
            continue

async def process_follow_event(client, event):
    """Process a single follow event asynchronously."""
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

async def process_profile_event(client, event):
    """Process a single profile update event asynchronously."""
    user_did = event.get('did')
    operation = event.get('commit', {}).get('operation')
    new_record = event.get('commit', {}).get('record', {})
    
    if not user_did or operation != 'update':
        return
        
    # Process the profile change
    changed_categories = await detect_profile_changes(user_did, new_record, client)
    
    if changed_categories:
        # Check if this user has mutual followers
        try:
            user_followers = client.get_followers(actor=user_did)
            bot_followers = client.get_followers(actor=client.me.did)
            
            user_follower_dids = {follower.did for follower in user_followers.followers}
            bot_follower_dids = {follower.did for follower in bot_followers.followers}
            
            mutual_followers = user_follower_dids.intersection(bot_follower_dids)
            
            if mutual_followers:
                print(f"📝 Profile changes detected: {', '.join(changed_categories)} for relevant user")
                user_profile = client.get_profile(actor=user_did)
                
                for follower_did in mutual_followers:
                    try:
                        user_prefs = await database.get_user_preferences(follower_did)
                        
                        # Filter changes based on user preferences
                        enabled_changes = []
                        for category in changed_categories:
                            if category not in user_prefs:
                                enabled_changes.append(category)
                        
                        if enabled_changes:
                            message = format_change_message(user_profile, enabled_changes)
                            send_dm(client, follower_did, message)
                            print(f"📬 Sent profile change notification to {follower_did}")
                            
                    except Exception as e:
                        print(f"❌ Error sending profile notification to {follower_did}: {e}")
            # Silently skip irrelevant users (no need to log spam)
                
        except Exception as e:
            # Handle "Actor not found" errors gracefully (deleted/suspended accounts)
            error_str = str(e)
            if "Actor not found" in error_str or "InvalidRequest" in error_str:
                print(f"ℹ️  Profile change for deleted/suspended account: {user_did} - skipped")
            else:
                print(f"❌ Error processing profile change for {user_did}: {e}")
    # Silently skip when no changes detected (no need to log spam)

async def check_followers_and_dms(client):
    """Periodically check for new DMs and respond to commands."""
    while True:
        try:
            # Check for new DMs
            dm_client = client.with_bsky_chat_proxy()
            convos_list = dm_client.chat.bsky.convo.list_convos()
            convos = convos_list.convos
            
            for convo in convos:
                try:
                    messages_list = dm_client.chat.bsky.convo.get_messages(
                        models.ChatBskyConvoGetMessages.Params(convo_id=convo.id)
                    )
                    messages = messages_list.messages
                    
                    # Get the latest message
                    if messages:
                        latest_message = messages[-1]
                        sender_did = latest_message.sender.did
                        
                        # Skip if this is a message from the bot itself
                        if sender_did == client.me.did:
                            continue
                        
                        message_text = latest_message.text.strip().lower()
                        
                        # Parse DM commands
                        if message_text.startswith('disable '):
                            category = message_text.replace('disable ', '').strip()
                            if category in VALID_CATEGORIES:
                                await database.update_user_preference(sender_did, category, True)
                                response = f"✅ Disabled {category} notifications for you."
                                send_dm(client, sender_did, response)
                                print(f"Disabled {category} for {sender_did}")
                            else:
                                response = f"❌ Invalid category '{category}'. Valid categories are: {', '.join(VALID_CATEGORIES)}"
                                send_dm(client, sender_did, response)
                        
                        elif message_text.startswith('enable '):
                            category = message_text.replace('enable ', '').strip()
                            if category in VALID_CATEGORIES:
                                await database.update_user_preference(sender_did, category, False)
                                response = f"✅ Enabled {category} notifications for you."
                                send_dm(client, sender_did, response)
                                print(f"Enabled {category} for {sender_did}")
                            else:
                                response = f"❌ Invalid category '{category}'. Valid categories are: {', '.join(VALID_CATEGORIES)}"
                                send_dm(client, sender_did, response)
                        
                        elif message_text in ['help', 'commands', '?']:
                            response = """Available commands:
• "disable [category]" - Stop notifications for a category
• "enable [category]" - Re-enable notifications for a category

Categories: avatar, displayname, bio, banner, handle

Example: "disable avatar" or "enable bio" """
                            send_dm(client, sender_did, response)
                        
                        # For unrecognized commands, provide help
                        elif message_text and not any(message_text.startswith(cmd) for cmd in ['disable ', 'enable ', 'help', 'commands', '?']):
                            response = """❌ Invalid command. Available commands:
• "disable [category]" - Stop notifications for a category  
• "enable [category]" - Re-enable notifications for a category

Categories: avatar, displayname, bio, banner, handle

Send "help" for more info."""
                            send_dm(client, sender_did, response)
                            
                except Exception as e:
                    print(f"Error processing DM conversation {convo.id}: {e}")
                    continue
            
            # Wait before checking again
            await asyncio.sleep(30)
            
        except Exception as e:
            print(f"Error in DM check loop: {e}")
            await asyncio.sleep(60)

async def main():
    # Initialize database connection
    await database.init_db()
    
    # Initialize AT Protocol client
    client = Client()
    client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_PASSWORD'))
    print(f"✅ Logged in as {client.me.handle}")
    
    # Run all listeners concurrently
    await asyncio.gather(
        listen_all_events(client),
        check_followers_and_dms(client)
    )

if __name__ == "__main__":
    asyncio.run(main())