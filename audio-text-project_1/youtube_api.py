# =====================================================
# youtube_api.py
# =====================================================

from googleapiclient.discovery import build
import isodate
import random

from config import (
    YOUTUBE_API_KEY,
    SEARCH_QUERIES,
    CHANNEL_IDS,
    MAX_RESULTS,
    MAX_VIDEO_DURATION
)

# =====================================================
# CREATE YOUTUBE CLIENT
# =====================================================

youtube = build(
    "youtube",
    "v3",
    developerKey=YOUTUBE_API_KEY
)

# =====================================================
# REJECT KEYWORDS
# =====================================================

REJECT_KEYWORDS = [
    "short",
    "shorts",
    "#shorts",

    "music",
    "song",
    "lyrics",
    "lyric",
    "cover",
    "remix",
    "dj",

    "status",
    "whatsapp",

    "tiktok",

    "karaoke",

    "instrumental",

    "reaction",

    "teaser",

    "trailer",

    "dance",

    "mv",

    "official video",

    "official music"
]

# =====================================================
# SEARCH USING QUERY
# =====================================================

def search_videos():
    query = random.choice(
        SEARCH_QUERIES
    )

    print(f"\nSearching Query: {query}")

    request = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=MAX_RESULTS,
        relevanceLanguage="si",
        safeSearch="none",
        order="relevance"
    )

    response = request.execute()

    return response.get(
        "items",
        []
    )

# =====================================================
# SEARCH TRUSTED CHANNELS
# =====================================================

def search_channel_videos():
    videos = []

    for channel_id in CHANNEL_IDS:
        if not channel_id or channel_id.strip() == "":
            continue

        print(f"\nSearching Channel: {channel_id.strip()}")

        request = youtube.search().list(
            part="snippet",
            channelId=channel_id.strip(),
            type="video",
            order="date",
            maxResults=MAX_RESULTS
        )

        response = request.execute()

        videos.extend(
            response.get(
                "items",
                []
            )
        )

    return videos

# =====================================================
# GET VIDEO DETAILS
# =====================================================

def get_video_details(video_id):

    request = youtube.videos().list(
        part="snippet,contentDetails,status",
        id=video_id
    )

    response = request.execute()

    # -------------------------------------------------
    # DEBUG OUTPUT
    # -------------------------------------------------

    print("\n" + "=" * 70)
    print("YOUTUBE API RESPONSE")
    print("=" * 70)
    print(response)

    if len(response.get("items", [])) == 0:
        print("No video found.")
        return None

    item = response["items"][0]

    print("\nVIDEO ITEM:")
    print(item)

    # -------------------------------------------------
    # SAFETY CHECKS
    # -------------------------------------------------

    if "contentDetails" not in item:
        print("ERROR: contentDetails missing")
        return None

    if "duration" not in item["contentDetails"]:
        print("ERROR: duration missing")
        print(item["contentDetails"])
        return None

    # -------------------------------------------------
    # GET DURATION
    # -------------------------------------------------

    try:
        duration = int(
            isodate.parse_duration(
                item["contentDetails"]["duration"]
            ).total_seconds()
        )
    except Exception as e:
        print(f"Error parsing duration: {e}")
        return None

    return {
        "video_id": video_id,
        "title": item["snippet"].get("title", ""),
        "channel": item["snippet"].get("channelTitle", ""),
        "channel_id": item["snippet"].get("channelId", ""),
        "language": item["snippet"].get(
            "defaultLanguage",
            ""
        ),
        "duration": duration,
        "url": f"https://www.youtube.com/watch?v={video_id}"
    }
# =====================================================
# TITLE FILTER
# =====================================================

def reject_title(title):
    title = title.lower()

    for keyword in REJECT_KEYWORDS:
        if keyword in title:
            return True

    return False

# =====================================================
# MOSTLY ENGLISH TITLE
# =====================================================

def mostly_english(title):
    sinhala = 0
    english = 0

    for ch in title:
        if '\u0D80' <= ch <= '\u0DFF':
            sinhala += 1
        elif ch.isalpha() and ord(ch) < 128:
            english += 1

    return english > sinhala

# =====================================================
# VALID VIDEOS
# =====================================================

def get_valid_videos():
    valid_videos = []

    # ------------------------------------------
    # SEARCH RESULTS
    # ------------------------------------------

    search_results = search_videos()

    print(
        f"\nSearch Results : {len(search_results)}"
    )

    # ------------------------------------------
    # CHANNEL RESULTS
    # ------------------------------------------

    channel_results = search_channel_videos()

    print(
        f"Channel Results : {len(channel_results)}"
    )

    # ------------------------------------------
    # MERGE RESULTS
    # ------------------------------------------

    all_results = search_results + channel_results

    # ------------------------------------------
    # REMOVE DUPLICATES (WITH SAFE ACCESS)
    # ------------------------------------------

    unique_results = {}

    for item in all_results:
        video_id = item.get("id", {}).get("videoId")

        if not video_id:
            continue

        if video_id not in unique_results:
            unique_results[video_id] = item

    all_results = list(unique_results.values())

    print(
        f"Unique Videos : {len(all_results)}"
    )

    # ------------------------------------------
    # FILTER VIDEOS
    # ------------------------------------------

    for result in all_results:
        video_id = result.get("id", {}).get("videoId")

        if not video_id:
            continue

        details = get_video_details(video_id)

        if details is None:
            continue

        print()
        print(f"Checking : {details['title']}")
        print(f"Channel  : {details['channel']}")
        print(f"Duration : {details['duration']} sec")

        # ------------------------------------------
        # DURATION
        # ------------------------------------------

        if details["duration"] > MAX_VIDEO_DURATION:
            print("✗ Rejected - Too Long")
            continue

        # ------------------------------------------
        # TITLE FILTER
        # ------------------------------------------

        if reject_title(details["title"]):
            print("✗ Rejected - Music/Shorts")
            continue

        # ------------------------------------------
        # ENGLISH FILTER
        # ------------------------------------------

        if mostly_english(details["title"]):
            print("✗ Rejected - Mostly English")
            continue

        print("✓ Accepted")

        valid_videos.append(details)

    return valid_videos

# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":
    videos = get_valid_videos()

    print()
    print("=" * 70)
    print(f"Valid Videos Found : {len(videos)}")
    print("=" * 70)

    for video in videos:
        print()
        print("Title       :", video["title"])
        print("Channel     :", video["channel"])
        print("Channel ID  :", video["channel_id"])
        print("Duration    :", video["duration"])
        print("Video ID    :", video["video_id"])
        print("URL         :", video["url"])