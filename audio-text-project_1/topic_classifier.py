# =====================================================
# topic_classifier.py
# =====================================================

import re


# =====================================================
# TOPIC KEYWORDS
# =====================================================

TOPIC_KEYWORDS = {

    "Sports": [
        "ක්‍රීඩා",
        "ක්‍රිකට්",
        "පාපන්දු",
        "ටෙනිස්",
        "තරගය",
        "තරඟය",
        "කණ්ඩායම",
        "ක්‍රීඩක",
        "ක්‍රීඩිකා",
        "ලකුණු",
        "ලෝක කුසලාන",
        "ඔලිම්පික්"
    ],

    "Politics": [
        "දේශපාලන",
        "දේශපාලනය",
        "ජනාධිපති",
        "අගමැති",
        "පාර්ලිමේන්තු",
        "පාර්ලිමේන්තුව",
        "මැතිවරණ",
        "ඡන්ද",
        "ආණ්ඩුව",
        "රජය",
        "පක්ෂය",
        "පක්ෂ",
        "මන්ත්‍රී",
        "ඇමති",
        "දේශපාලක"
    ],

    "News": [
        "ප්‍රවෘත්ති",
        "පුවත්",
        "වාර්තා",
        "සිදුවීම",
        "සිද්ධිය",
        "අනතුර",
        "පොලිසිය",
        "අත්අඩංගුවට",
        "මරණය",
        "මියගිය",
        "ගංවතුර",
        "භූමිකම්පා"
    ],

    "Education": [
        "අධ්‍යාපන",
        "අධ්‍යාපනය",
        "පාසල",
        "පාසල්",
        "විශ්වවිද්‍යාල",
        "විභාග",
        "ශිෂ්‍ය",
        "ශිෂ්‍යය",
        "ගුරුවර",
        "දේශනය",
        "ඉගෙන",
        "පන්ති"
    ],

    "Technology": [
        "තාක්ෂණ",
        "තාක්ෂණික",
        "පරිගණක",
        "මෘදුකාංග",
        "දෘඩාංග",
        "අන්තර්ජාල",
        "ජංගම",
        "කෘතිම බුද්ධිය",
        "කෘත්‍රිම බුද්ධිය",
        "ඇප්",
        "යෙදුම්"
    ],

    "Health": [
        "සෞඛ්‍ය",
        "රෝග",
        "රෝගී",
        "වෛද්‍ය",
        "වෛද්‍යවර",
        "රෝහල",
        "ඖෂධ",
        "ප්‍රතිකාර",
        "කොවිඩ්",
        "ආහාර",
        "පෝෂණ"
    ],

    "Business": [
        "ව්‍යාපාර",
        "ව්‍යාපාරික",
        "ආර්ථික",
        "ආර්ථිකය",
        "වෙළඳපොළ",
        "වෙළඳ",
        "මිල",
        "බැංකු",
        "ආයෝජන",
        "රැකියා",
        "මුදල්"
    ],

    "Entertainment": [
        "විනෝදාස්වාද",
        "චිත්‍රපට",
        "සිනමා",
        "ටෙලිනාට්‍ය",
        "නළුව",
        "නිළිය",
        "ගායක",
        "ගායික",
        "රංගන",
        "කලාකර"
    ],

    "Religion": [
        "ආගම",
        "ආගමික",
        "බුදු",
        "බුද්ධ",
        "පන්සල",
        "පන්සල්",
        "භික්ෂු",
        "භික්ෂූ",
        "ධර්ම",
        "පිරිත්",
        "පූජා"
    ]
}


# =====================================================
# HASHTAG KEYWORDS
# =====================================================
# These are used only when hashtags appear in the
# YouTube video title.
#
# Hashtags are converted to lowercase before matching.
# Both English and Sinhala hashtags are supported.
# =====================================================

HASHTAG_KEYWORDS = {

    "Sports": [
        "#sports",
        "#sport",
        "#cricket",
        "#football",
        "#soccer",
        "#tennis",
        "#srilankasports",
        "#ක්‍රීඩා",
        "#ක්‍රිකට්",
        "#පාපන්දු",
        "#ටෙනිස්"
    ],

    "Politics": [
        "#politics",
        "#srilankapolitics",
        "#government",
        "#election",
        "#parliament",
        "#දේශපාලනය",
        "#දේශපාලන",
        "#මැතිවරණ",
        "#ඡන්ද",
        "#පාර්ලිමේන්තුව"
    ],

    "News": [
        "#news",
        "#srilankanews",
        "#breakingnews",
        "#currentaffairs",
        "#ප්‍රවෘත්ති",
        "#පුවත්",
        "#ප්‍රවෘත්ති",
        "#වාර්තා"
    ],

    "Education": [
        "#education",
        "#learning",
        "#school",
        "#university",
        "#student",
        "#අධ්‍යාපනය",
        "#අධ්‍යාපන",
        "#පාසල්",
        "#විශ්වවිද්‍යාල",
        "#විභාග"
    ],

    "Technology": [
        "#technology",
        "#tech",
        "#artificialintelligence",
        "#ai",
        "#software",
        "#computer",
        "#තාක්ෂණ",
        "#තාක්ෂණික",
        "#පරිගණක",
        "#මෘදුකාංග"
    ],

    "Health": [
        "#health",
        "#healthcare",
        "#medicine",
        "#medical",
        "#wellness",
        "#සෞඛ්‍ය",
        "#වෛද්‍ය",
        "#රෝග",
        "#රෝහල",
        "#ඖෂධ"
    ],

    "Business": [
        "#business",
        "#economy",
        "#finance",
        "#investment",
        "#banking",
        "#ව්‍යාපාර",
        "#ආර්ථික",
        "#වෙළඳපොළ",
        "#බැංකු",
        "#ආයෝජන"
    ],

    "Entertainment": [
        "#entertainment",
        "#cinema",
        "#movie",
        "#film",
        "#actor",
        "#actress",
        "#විනෝදාස්වාද",
        "#චිත්‍රපට",
        "#සිනමා",
        "#ටෙලිනාට්‍ය"
    ],

    "Religion": [
        "#religion",
        "#buddhism",
        "#buddha",
        "#temple",
        "#ආගම",
        "#ආගමික",
        "#බුදු",
        "#බුද්ධ",
        "#පන්සල",
        "#ධර්ම"
    ]
}


# =====================================================
# TEXT NORMALIZATION
# =====================================================

def normalize_text(text):

    return re.sub(
        r"\s+",
        " ",
        str(text or "").strip().lower()
    )


# =====================================================
# EXTRACT HASHTAGS FROM TITLE
# =====================================================

def extract_hashtags(title):

    title = str(title or "")

    hashtags = re.findall(
        r"#[\w\u0D80-\u0DFF]+",
        title,
        flags=re.UNICODE
    )

    return [
        normalize_text(hashtag)
        for hashtag in hashtags
    ]


# =====================================================
# CLASSIFY TOPIC
# =====================================================

def classify_video_topic(
    title="",
    transcript=""
):

    title = normalize_text(title)
    transcript = normalize_text(transcript)

    # Extract hashtags from the original title
    hashtags = extract_hashtags(title)

    scores = {}

    for topic, keywords in TOPIC_KEYWORDS.items():

        score = 0

        # -------------------------------------------------
        # NORMAL KEYWORDS
        # -------------------------------------------------

        for keyword in keywords:

            keyword = normalize_text(
                keyword
            )

            # Keyword found in transcript
            if keyword in transcript:
                score += 2

            # Keyword found in title
            if keyword in title:
                score += 3

        # -------------------------------------------------
        # HASHTAGS
        # -------------------------------------------------

        hashtag_keywords = HASHTAG_KEYWORDS.get(
            topic,
            []
        )

        for hashtag in hashtags:

            for keyword in hashtag_keywords:

                keyword = normalize_text(
                    keyword
                )

                if hashtag == keyword:
                    score += 4

        scores[topic] = score

    # -----------------------------------------------------
    # NO TOPICS
    # -----------------------------------------------------

    if not scores:
        return "Other"

    # -----------------------------------------------------
    # HIGHEST SCORING TOPIC
    # -----------------------------------------------------

    best_topic = max(
        scores,
        key=scores.get
    )

    # -----------------------------------------------------
    # NO MATCH
    # -----------------------------------------------------

    if scores[best_topic] == 0:
        return "Other"

    return best_topic


# =====================================================
# VIDEO HELPER
# =====================================================

def classify_topic_from_video(
    video,
    transcript=""
):

    return classify_video_topic(
        title=video.get(
            "title",
            ""
        ),
        transcript=transcript
    )


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    examples = [

        (
            "ශ්‍රී ලංකා ක්‍රිකට් කණ්ඩායම",
            "අද තරගයේදී කණ්ඩායම ලකුණු ලබා ගත්තා"
        ),

        (
            "නව අධ්‍යාපන වැඩසටහන",
            "විශ්වවිද්‍යාල ශිෂ්‍යයන් සඳහා නව අධ්‍යාපන ක්‍රමයක්"
        ),

        (
            "නව තාක්ෂණික වැඩසටහන",
            "කෘත්‍රිම බුද්ධිය සහ පරිගණක තාක්ෂණය"
        ),

        (
            "Sri Lanka Cricket Discussion #Cricket #Sports",
            "අද තරඟය ගැන අපි කතා කරමු"
        ),

        (
            "New University Program #Education",
            "විශ්වවිද්‍යාල ශිෂ්‍යයන් සඳහා නව වැඩසටහනක්"
        ),

        (
            "AI Technology Discussion #Technology #AI",
            "කෘත්‍රිම බුද්ධිය සහ පරිගණක තාක්ෂණය"
        )
    ]

    for title, transcript in examples:

        print(
            "Title:",
            title
        )

        print(
            "Transcript:",
            transcript
        )

        print(
            "Topic:",
            classify_video_topic(
                title,
                transcript
            )
        )

        print(
            "-" * 60
        )