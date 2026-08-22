# =====================================================
# collector.py
# =====================================================

from youtube_api import get_valid_videos

from database import (
    is_processed,
    save_video
)

# =====================================================
# GET NEXT VIDEO
# =====================================================

def get_next_video():

    """
    Search YouTube and return the first
    valid video that is not already
    stored in the database.
    """

    videos = get_valid_videos()

    for video in videos:

        # --------------------------------------------
        # CHECK DUPLICATE
        # --------------------------------------------

        if is_processed(video["video_id"]):

            print(
                f"Skipping duplicate: {video['title']}"
            )

            continue

        # --------------------------------------------
        # SAVE VIDEO TO DATABASE
        # --------------------------------------------

        save_video(

            video["video_id"],

            video["title"],

            video["url"],

            video["duration"]

        )

        print("\nSelected New Video")
        print("-" * 40)
        print(f"Title    : {video['title']}")
        print(f"Duration : {video['duration']} seconds")
        print(f"Video ID : {video['video_id']}")
        print(f"URL      : {video['url']}")

        return video

    print("\nNo new videos available.")

    return None


# =====================================================
# TEST
# =====================================================

if __name__ == "__main__":

    video = get_next_video()

    if video:

        print("\nReturned Video:")
        print(video)