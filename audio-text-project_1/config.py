# =====================================================
# config.py
# =====================================================

# =====================================================
# YOUTUBE API
# =====================================================

YOUTUBE_API_KEY = ""

# =====================================================
# SEARCH SETTINGS
# =====================================================

SEARCH_QUERIES = [

    # Interviews
    "සිංහල සම්මුඛ සාකච්ඡාව",
  

    # Discussions
    "සිංහල සාකච්ඡාව",
   

    # Lectures
    "සිංහල දේශනය",
    "Sinhala lecture",

    # Educational
    "සිංහල අධ්‍යාපනික වැඩසටහන",
    

    # Podcasts
    "සිංහල පොඩ්කාස්ට්",
    "Sinhala podcast",

    # University
    "සිංහල විශ්වවිද්‍යාල දේශනය",
    

    # News discussions
    "සිංහල ප්‍රවෘත්ති සාකච්ඡාව",
    "Sinhala news interview",

    # Public talks
    "සිංහල සංවාදය",
    "සිංහල කතාව",

    # Technology
    "සිංහල තාක්ෂණික දේශනය",

    # Science
    "සිංහල විද්‍යාත්මක දේශනය"

]


CHANNEL_IDS = [

   
    "UCGoPNaJqz0fMH7vcGzq_2xw",

    # Ada Derana
    "",

    # Hiru News
    "UCckltLEhFLv8Xz_lQhYfwmg",

    

    # Swarnavahini
    "UCaIc6SgS90ud_RgMSC6hW_w",

]
# Number of search results retrieved
MAX_RESULTS = 25

# Maximum duration (seconds)
MAX_VIDEO_DURATION = 180

# Number of videos downloaded per pipeline run
VIDEOS_PER_RUN = 1

# =====================================================
# DATABASE
# =====================================================
DATABASE_NAME = "data/videos.db"


# =====================================================
# GOOGLE DRIVE DESKTOP
# =====================================================

GOOGLE_DRIVE_FOLDER = r"G:\My Drive\Sinhala Dataset Clips"