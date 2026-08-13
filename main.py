from news_pipeline.cli import main


if __name__ == "__main__":
    import sys

    # Preserve ``python main.py`` as the normal full run while allowing
    # explicit CLI arguments such as ``run --no-gpt`` and ``status``.
    main(sys.argv[1:] or ["run"])
