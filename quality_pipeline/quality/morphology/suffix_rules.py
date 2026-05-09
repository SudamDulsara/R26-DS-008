"""
suffix_rules.py
---------------
Sinhala suffix inventory used by the morphological analyzer.

This is a *data file*, not logic. New patterns get appended here; the analyzer
in analyzer.py picks them up automatically. Suffixes are grouped by category
so the scorer can reason about morphological *diversity* (how many different
*kinds* of suffixes a document uses), not just count them.

Sources for the rules (cite in your report):
  - Hettige & Karunananda (2006), "A Morphological Analyzer to Enable English
    to Sinhala Machine Translation"
  - Welgama, Weerasinghe & Niranjan (2013), "Evaluating a Machine Learning
    Approach to Sinhala Morphological Analysis"
  - Mallikadevi (2025), "A Comparative Study of the Genitive and Locative
    Cases in Tamil and Sinhala"
  - Standard Sinhala grammar references for verb conjugation tables

NOTE: Sinhala has *spoken* and *literary* registers with overlapping but
non-identical suffix sets. The rules below cover the **literary/written**
register, which is what the LLM training corpus targets. We deliberately
omit colloquial-only forms — flagging them is a future stage's job, not ours.

NOTE: Order within each list matters — the analyzer uses *longest-match*,
so longer suffixes must be attempted before shorter ones that are prefixes
of them (e.g., "ගෙන්" before "න්"). Lists are pre-sorted at the bottom of
this file.
"""

# ---------------------------------------------------------------------------
# 1. NOUN CASE MARKERS (singular)
# ---------------------------------------------------------------------------
# Sinhala nouns inflect for case. The five primary cases (Mallikadevi 2025;
# Hettige & Karunananda 2006) are: nominative, accusative, genitive, dative,
# ablative. The nominative is unmarked (no suffix), so we don't list it.

CASE_MARKERS_SINGULAR = [
    # Genitive — "of X" / possessive
    "ගේ",      # -ge: animate nouns ("මනුෂ්‍යයාගේ" = of the human)
    "එහි",     # -ehi: inanimate nouns, also locative
    "වල",      # -vala: plural inanimate genitive
    "ගෙ",      # -ge variant

    # Dative — "to X"
    "ට",       # -ṭa: dative ("ගෙදරට" = to the house)

    # Ablative / Instrumental — "from X" / "by X"
    "ගෙන්",    # -gen: ablative animate
    "න්",      # -in/-en: ablative/instrumental inanimate
    "කින්",    # -kin: indefinite ablative
    "කෙන්",    # -ken: indefinite ablative variant

    # Locative — "in X"
    "හි",      # -hi: locative
    "ේ",       # -e: locative (literary)
]

# ---------------------------------------------------------------------------
# 2. NUMBER MARKERS (plural)
# ---------------------------------------------------------------------------
# Sinhala plural forms are highly irregular. We list the productive
# (still-used to build new plurals) suffixes only. Reference: Languages
# Gulper grammar summary; Hettige & Karunananda (2006).

PLURAL_MARKERS = [
    "ලා",      # -lā: plural for kinship/proper/pronouns ("අම්මලා" = mothers)
    "වරු",     # -varu: honorific plural ("ගුරුවරු" = teachers)
    "වල්",     # -val: inanimate plural marker
    "හු",      # -hu: animate masculine plural
    "න්",      # -n: oblique plural stem (overlaps with case markers — handled
               #      by analyzer category-aware logic, not order alone)
]

# ---------------------------------------------------------------------------
# 3. DEFINITENESS / INDEFINITENESS MARKERS
# ---------------------------------------------------------------------------
# Sinhala has overt indefinite markers; definiteness is the unmarked default.
# Reference: AX NLG Sinhala documentation; standard grammar.

DEFINITENESS_MARKERS = [
    "ක්",      # -ak: indefinite inanimate ("පොතක්" = a book)
    "ෙක්",     # -ek: indefinite animate masculine ("මිනිහෙක්" = a man)
    "කු",      # -ku: indefinite animate accusative
    "යක්",     # -yak: indefinite with epenthetic y
]

# ---------------------------------------------------------------------------
# 4. VERB TENSE / PERSON SUFFIXES
# ---------------------------------------------------------------------------
# Sinhala verbs inflect for tense (past/present/future), person, and number.
# This is a SIMPLIFIED set covering the high-frequency forms — full conjugation
# tables are large and gana-class (verb-class) dependent. Reference: Hettige &
# Karunananda (2006) verb morphology section.

VERB_SUFFIXES = [
    # Present / habitual
    "නවා",     # -nawa: present habitual ("කරනවා" = does/is doing)
    "යි",      # -yi: present third person ("කරයි" = (he/she/it) does)

    # Past
    "වා",      # -wa: past tense marker (in some classes)
    "ුවා",     # -uwa: past with stem vowel
    "ුවේ",     # -uwe: past concessive/relative
    "ුණා",     # -unā: past intransitive

    # Future / volitive
    "වි",      # -wi: future
    "න්න",     # -nna: infinitive ("කරන්න" = to do)

    # Negative / participial
    "නේ",      # -ne: emphatic / cleft
    "මින්",    # -min: present participle ("කරමින්" = while doing)
    "ලා",      # -lā: past participle ("කරලා" = having done) — note overlap
               #      with plural -lā; analyzer disambiguates by context
]

# ---------------------------------------------------------------------------
# 5. EMPHATIC / DISCOURSE PARTICLES (clitics)
# ---------------------------------------------------------------------------
# These attach to the end of any inflected form. They're lower priority for
# the analyzer (stripped last) but contribute to morphological diversity.

CLITICS = [
    "ද",       # -da: question marker
    "ත්",      # -t: also/too
    "ම",       # -ma: emphatic
    "මයි",     # -mayi: emphatic + copula
]


# ---------------------------------------------------------------------------
# Aggregated, category-tagged suffix list used by the analyzer
# ---------------------------------------------------------------------------
# The analyzer needs to know not just WHAT suffixes exist but WHICH CATEGORY
# each belongs to, because the quality scorer measures *category diversity*.
# A doc that uses 50 case markers but no verb suffixes is morphologically
# different from one that uses 10 case markers + 10 verb suffixes + 5 plural
# markers, even if the total morpheme count is the same.

SUFFIX_TABLE: list[tuple[str, str]] = (
    [(s, "case")        for s in CASE_MARKERS_SINGULAR] +
    [(s, "plural")      for s in PLURAL_MARKERS] +
    [(s, "definite")    for s in DEFINITENESS_MARKERS] +
    [(s, "verb")        for s in VERB_SUFFIXES] +
    [(s, "clitic")      for s in CLITICS]
)

# Sort by length descending — analyzer uses longest-match-first.
SUFFIX_TABLE.sort(key=lambda pair: len(pair[0]), reverse=True)

# All recognized categories (for reporting/diversity calculations).
CATEGORIES = ("case", "plural", "definite", "verb", "clitic")