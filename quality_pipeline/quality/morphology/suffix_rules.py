"""
suffix_rules.py
---------------
Sinhala suffix inventory used by the morphological analyzer.

This is a *data file*, not logic. New patterns get appended here; the analyzer
in analyzer.py picks them up automatically. Suffixes are grouped by category
so the scorer can reason about morphological *diversity* (how many different
*kinds* of suffixes a document uses), not just count them.

Sources for the rules (verified citations — safe to put in the report):

  - Hettige, B. & Karunananda, A. S. (2006). "A Morphological Analyzer to
    Enable English to Sinhala Machine Translation." Proceedings of the 2nd
    International Conference on Information and Automation (ICIA 2006),
    Colombo, Sri Lanka, pp. 21-26.
    [Reported ~96% accuracy on Sinhala morphological analysis. No public
     data or code released — see de Silva (2019) survey.]

  - Welgama, V., Weerasinghe, R. & Niranjan, M. (2013). "Evaluating a
    Machine Learning Approach to Sinhala Morphological Analysis."
    Proceedings of the 10th International Conference on Natural Language
    Processing (ICON 2013), Noida, India.
    [Baseline worth quoting: unsupervised morph segmentation decomposed only
     35% of words in a linguistically accurate way, and identified the
     correct stem for 50%. Useful comparison point for our decomposition
     rate — it shows the task is genuinely hard, not that we are sloppy.]

  - Narayanan, Mallikadevi (2025). "A Comparative Study of the Genitive and
    Locative Cases in Tamil and Sinhala." International Journal of Research
    and Innovation in Social Science (IJRISS), Vol. IX, Issue VIII,
    pp. 5373-5380. DOI: 10.47772/IJRISS.2025.908000434.
    [Source for the genitive/locative markers -gē, -ē, -ehi, -vala, and the
     animacy split: animate nouns take -gē; inanimate singular takes -ē or
     -ehi; inanimate plural takes -vala.]

  - Nandathilaka, M., Ahangama, S. & Weerasuriya, G. T. (2018). "A Rule-based
    Lemmatizing Approach for Sinhala Language." 3rd International Conference
    on Information Technology Research (ICITR), IEEE.
    [Directly comparable prior work: rule-based suffix stripping, 77.3%
     accuracy, built on social-media data. Cite this to show rule-based
     stripping is an established method, not a shortcut.]

  - de Silva, N. (2019). "Survey on Publicly Available Sinhala Natural
    Language Processing Tools and Research." arXiv:1906.02358.
    [Use for the "no public Sinhala morphological analyzer exists" claim —
     it documents that Hettige & Karunananda, Welgama et al., and the
     SinMorphy line all lack released data or code.]

  - Gair, J. W. & Paolillo, J. C. (1997). Sinhala. Languages of the World /
    Materials 34. Lincom Europa.
    [Standard reference for the case system and the animacy / definiteness
     distinctions that drive form selection.]

REGISTER
--------
Sinhala is diglossic: spoken (colloquial) and literary (written) Sinhala
differ in *verb morphology*, not just vocabulary. This table targets the
**literary/written** register, which is what news, OCR'd documents, and the
LLM training corpus mostly contain.

Literary forms therefore dominate the verb list: -ේය, -ති, -ිණි, -මි, -මු,
-ාහ. The spoken present -නවා is retained because ASR transcripts are in the
spoken register and would otherwise score artificially low — but it is
tagged so the scorer can tell the two apart if we ever add register-aware
weighting (see quality/semantic_overlap.py for why register matters).

ORDERING
--------
The analyzer uses *longest-match-first*. SUFFIX_TABLE is sorted by codepoint
length descending at the bottom of this file, so "ගෙන්" is attempted before
"න්". Do not rely on the order within the individual category lists below —
they are grouped for human readability only.

A NOTE ON COVERAGE VS. DISCRIMINATION
-------------------------------------
Adding suffixes raises the decomposition rate of *all* text, including OCR
garbage that ends in a common grapheme by chance. The metric that matters is
the GAP between clean and degraded text, not the absolute score. Before and
after editing this file, re-run the scorer on the sample corpus and check
that clean news still separates from OCR/ASR output. If the gap narrows, the
new rules are hurting, not helping. LOW_CONFIDENCE_SUFFIXES and MIN_ROOT_LEN
at the bottom exist to control this.
"""

# ---------------------------------------------------------------------------
# 1. NOUN CASE MARKERS
# ---------------------------------------------------------------------------
# Sinhala nouns inflect for case. Nominative is unmarked, so it is not listed.
# Form selection depends on animacy and number (Mallikadevi 2025; Gair &
# Paolillo 1997): animate nouns take -gē for genitive, inanimate singular
# takes -ē / -ehi, inanimate plural takes -vala.

CASE_MARKERS = [
    # --- Genitive: "of X" / possessive ---
    "ගේ",       # -gē: animate ("මනුෂ්‍යයාගේ" = of the human)
    "ගෙ",       # -ge: reduced variant
    "එහි",      # -ehi: inanimate, also locative
    "ෙහි",      # -ehi: same marker after a consonant + e-sign
    "වල",       # -vala: plural inanimate genitive

    # --- Dative: "to X" ---
    "ට",        # -ṭa: dative ("ගෙදරට" = to the house)
    "හට",       # -haṭa: literary animate dative ("ළමයාහට")      [NEW]
    "න්ට",      # -nṭa: plural animate dative ("ළමයින්ට")        [NEW]
    "වලට",      # -valaṭa: plural inanimate dative ("ගම්වලට")     [NEW]

    # --- Ablative / Instrumental: "from X" / "by X" ---
    "ගෙන්",     # -gen: ablative animate
    "න්",       # -in / -en: ablative-instrumental inanimate
    "ෙන්",      # -en: same, after consonant + e-sign ("පොතෙන්")  [NEW]
    "කින්",     # -kin: indefinite ablative
    "කෙන්",     # -ken: indefinite ablative variant
    "වලින්",    # -valin: plural inanimate ablative ("පොත්වලින්")  [NEW]

    # --- Locative: "in / at X" ---
    "හි",       # -hi: locative
    "ේ",        # -ē: locative (literary)  ** see LOW_CONFIDENCE **
    "දී",       # -dī: locative / temporal ("ගමේදී" = at the village) [NEW]

    # --- Vocative (literary, common in formal prose) ---
    "නි",       # -ni: vocative ("මිත්‍රයනි" = O friends)          [NEW]
    "ෙනි",      # -eni: vocative variant                          [NEW]
]

# ---------------------------------------------------------------------------
# 2. NUMBER MARKERS (plural)
# ---------------------------------------------------------------------------
# Sinhala plurals are highly irregular; many are formed by phonemic alternation
# of the stem-final vowel rather than by suffixation, which a suffix stripper
# structurally cannot catch. We list the *productive* suffixes only.

PLURAL_MARKERS = [
    "ලා",       # -lā: kinship / proper / pronoun plural ("අම්මලා")
    "වරු",      # -varu: honorific plural ("ගුරුවරු" = teachers)
    "වරුන්",    # -varun: honorific plural, oblique stem            [NEW]
    "වල්",      # -val: inanimate plural
    "හු",       # -hu: animate masculine plural (literary)
    "ෝ",        # -ō: animate plural ("මිනිසෝ", "ගොවියෝ")           [NEW]
    # NOTE: "න්" (oblique plural stem) is deliberately NOT repeated here.
    # It lives in CASE_MARKERS and is declared ambiguous below, so the
    # analyzer can report both readings instead of silently picking one.
]

# ---------------------------------------------------------------------------
# 3. DEFINITENESS / INDEFINITENESS MARKERS
# ---------------------------------------------------------------------------
# Sinhala has overt indefinite markers; definiteness is the unmarked default.
# The animate/inanimate split matters here (Gair & Paolillo 1997).

DEFINITENESS_MARKERS = [
    "ක්",       # -ak: indefinite inanimate ("පොතක්" = a book)
    "යක්",      # -yak: indefinite inanimate with epenthetic y
    "ෙක්",      # -ek: indefinite animate masculine ("මිනිහෙක්")
    "යෙක්",     # -yek: indefinite animate with epenthetic y ("ගොවියෙක්") [NEW]
    "කු",       # -ku: indefinite animate accusative
    "ෙකු",      # -eku: indefinite animate accusative variant       [NEW]
    "යකු",      # -yaku: same with epenthetic y                     [NEW]
]

# ---------------------------------------------------------------------------
# 4. VERB TENSE / PERSON / MOOD SUFFIXES
# ---------------------------------------------------------------------------
# Sinhala verbs inflect for tense, person, number, gender and volition. Full
# conjugation is gana-class dependent and very large; this covers the
# high-frequency forms, weighted toward the LITERARY register because that is
# what written corpora contain.
#
# Reference: Hettige & Karunananda (2006) verb morphology section; standard
# grammar references (Gunasekara, A Comprehensive Grammar of the Sinhalese
# Language; Karunathilaka, Sinhala Bhasha Vyakaranaya).

VERB_SUFFIXES = [
    # --- Present / habitual ---
    "නවා",      # -nawā: present habitual — SPOKEN register ("කරනවා")
    "යි",       # -yi: present 3rd person — literary ("කරයි")
    "ෙයි",      # -eyi: present 3sg variant ("වෙයි")                [NEW]
    "ති",       # -ti: present 3rd person PLURAL — literary ("කරති") [NEW]
    "ෙති",      # -eti: same, after e-sign                          [NEW]
    "මි",       # -mi: 1st person singular — literary ("කරමි")      [NEW]
    "මු",       # -mu: 1st person plural — literary ("කරමු")        [NEW]

    # --- Past ---
    "වා",       # -wā: past tense marker (some classes)
    "ුවා",      # -uwā: past with stem vowel
    "ුවේ",      # -uwē: past concessive / relative
    "ුණා",      # -uṇā: past intransitive
    "ේය",       # -ēya: literary past 3sg masc ("කෙළේය", "ගියේය")   [NEW]
    "ීය",       # -īya: literary past variant                       [NEW]
    "ාහ",       # -āha: literary past plural ("කළාහ", "ගියාහ")      [NEW]
    "ිණි",      # -iṇi: literary past ("ලැබිණි" = was received)     [NEW]
    "ුණි",      # -uṇi: literary past passive ("කරනු ලැබුණි")       [NEW]

    # --- Future / volitive / optative ---
    "වි",       # -wi: future
    "ේවා",      # -ēwā: optative ("වේවා" = may it be)              [NEW]

    # --- Infinitive / non-finite ---
    "න්න",      # -nna: infinitive ("කරන්න" = to do)
    "න්නට",     # -nnaṭa: infinitive + dative ("කරන්නට")            [NEW]
    "න්නා",     # -nnā: agentive / habitual participle ("කරන්නා")   [NEW]
    "න්නී",     # -nnī: agentive, feminine                          [NEW]
    "නු",       # -nu: passive / imperative particle ("කරනු")       [NEW]
    "ූ",        # -ū: literary past adjectival participle ("වූ")    [NEW]

    # --- Participial / converb ---
    "නේ",       # -nē: emphatic / cleft
    "මින්",     # -min: present participle ("කරමින්" = while doing)
    "ෙමින්",    # -emin: same, after e-sign                         [NEW]
    "ගෙන",      # -gena: converb, "having done" ("කරගෙන")           [NEW]
    "ලා",       # -lā: past participle ("කරලා") — AMBIGUOUS with plural -lā

    # --- Conditional / quotative ---
    "හොත්",     # -hot: conditional ("කළහොත්" = if done)            [NEW]
    "යැයි",     # -yæyi: quotative ("කරයැයි" = saying that ...)     [NEW]
]

# ---------------------------------------------------------------------------
# 5. DERIVATIONAL SUFFIXES                                            [NEW]
# ---------------------------------------------------------------------------
# Everything above is INFLECTIONAL — it changes the grammatical form of a word
# without changing its part of speech. These are DERIVATIONAL: they build new
# words, usually abstract nouns from adjectives or nouns.
#
# Why this category earns its place: derivational morphology is a marker of
# *register and complexity*. Formal written Sinhala (government documents,
# editorials, academic prose) is dense with -tvaya / -bhāvaya abstractions;
# ASR transcripts of casual speech have almost none. So this category gives
# the scorer a signal the inflectional categories cannot provide, and it
# widens the range of the category-diversity feature from 5 to 6.

DERIVATIONAL_SUFFIXES = [
    "කම",       # -kama: abstract noun from adjective ("හොඳකම" = goodness)
    "කම්",      # -kam: plural of the above
    "ත්වය",     # -tvaya: abstract ("මානවත්වය" = humanity)
    "භාවය",     # -bhāvaya: abstract state ("සතුටුභාවය" = happiness)
    "තාවය",     # -tāvaya: abstract quality
    "තාව",      # -tāva: abstract quality, shorter form
    "වන්ත",     # -vanta: possessive adjective ("ගුණවන්ත" = virtuous)
    "මය",       # -maya: "made of / consisting of" ("ස්වර්ණමය")
    "ාව",       # -āva: nominaliser ("ක්‍රියාව" = action) ** LOW CONFIDENCE **
]

# ---------------------------------------------------------------------------
# 6. EMPHATIC / DISCOURSE PARTICLES (clitics)
# ---------------------------------------------------------------------------
# These attach to the end of an already-inflected form. They are stripped last
# by the analyzer, but they contribute to morphological diversity and are a
# good signal of natural connected text (OCR garbage rarely produces them in
# the right positions).

CLITICS = [
    "ද",        # -da: question marker
    "ත්",       # -t: also / too
    "ම",        # -ma: emphatic
    "මයි",      # -mayi: emphatic + copula
    "මැයි",     # -mæyi: emphatic variant                           [NEW]
    "ය",        # -ya: literary sentence-final particle             [NEW]
                #      ** very high frequency in written Sinhala, but also
                #      the single most dangerous entry in this file — see
                #      LOW_CONFIDENCE_SUFFIXES **
    "ලු",       # -lu: hearsay / reportative ("කරලු" = apparently did) [NEW]
    "වත්",      # -vat: "even / at least"                           [NEW]
    "දෝ",       # -dō: dubitative ("කෙසේදෝ")                        [NEW]
]


# ===========================================================================
# Table construction
# ===========================================================================

# Category priority for duplicate resolution. When the same surface string
# appears in more than one category list, the FIRST category in this tuple
# wins and becomes the canonical tag. The alternative readings are preserved
# in AMBIGUOUS_SUFFIXES so the analyzer can report them rather than losing
# them silently.
#
# Order rationale: inflectional categories that carry more grammatical
# information rank above ones that carry less; clitics rank last because they
# are the most promiscuous (they attach to anything).
CATEGORY_PRIORITY = ("case", "verb", "plural", "definite", "derivational", "clitic")

_CATEGORY_LISTS = {
    "case":         CASE_MARKERS,
    "plural":       PLURAL_MARKERS,
    "definite":     DEFINITENESS_MARKERS,
    "verb":         VERB_SUFFIXES,
    "derivational": DERIVATIONAL_SUFFIXES,
    "clitic":       CLITICS,
}

# Every category each surface form can belong to, in priority order.
_readings: dict[str, list[str]] = {}
for _cat in CATEGORY_PRIORITY:
    for _s in _CATEGORY_LISTS[_cat]:
        _readings.setdefault(_s, [])
        if _cat not in _readings[_s]:
            _readings[_s].append(_cat)

# Surface forms with more than one possible category. The analyzer should
# ideally disambiguate these using context (what the remaining stem looks
# like, what was stripped before it). Until it does, it uses the canonical
# tag and the scorer should not treat these as confident evidence of the
# canonical category.
AMBIGUOUS_SUFFIXES: dict[str, tuple[str, ...]] = {
    s: tuple(cats) for s, cats in _readings.items() if len(cats) > 1
}

# The table the analyzer consumes: (surface, canonical_category), deduplicated.
SUFFIX_TABLE: list[tuple[str, str]] = [
    (s, cats[0]) for s, cats in _readings.items()
]

# Longest-match-first. Sorting by codepoint length is correct here because the
# analyzer matches on raw strings, not grapheme clusters. Secondary sort on the
# string itself keeps the table stable across Python runs (useful for tests).
SUFFIX_TABLE.sort(key=lambda pair: (-len(pair[0]), pair[0]))

# All recognised categories, for reporting and diversity calculations.
CATEGORIES = CATEGORY_PRIORITY


# ===========================================================================
# Guards against score inflation
# ===========================================================================
# These exist because expanding the table makes it EASIER for any string to
# decompose, including corrupted text. The analyzer should honour them.

# Minimum codepoints that must remain after stripping a suffix. Without this,
# a two-character OCR fragment ending in "ය" decomposes "successfully" and
# inflates the document's decomposition rate.
#
# Tuned empirically to 2, not 3. Three looks safer but breaks real analysis:
# many Sinhala verb roots are two codepoints (කර "do", බල "look", ගි "go"), so
# MIN_ROOT_LEN = 3 blocks "කරනවා" -> කර + නවා and forces the analyzer to fall
# through to a shorter, WRONG suffix (කරන + වා). A guard that produces bad
# parses is worse than no guard. Two is the floor that keeps correct
# decompositions intact.
MIN_ROOT_LEN = 2

# Single-grapheme and highly promiscuous suffixes. These match by chance far
# more often than the longer ones, so a decomposition that consists ONLY of
# these should count for less. Recommended use in scorer.py: count them at a
# reduced weight (e.g. 0.5) when computing decomposition rate, and exclude
# them from the "agglutination depth" feature entirely.
LOW_CONFIDENCE_SUFFIXES = frozenset({
    "ය",    # literary sentence-final — matches an enormous number of strings
    "ම",    # emphatic
    "ද",    # question marker
    "ට",    # dative
    "ේ",    # locative / verb ending, single vowel sign
    "ෝ",    # animate plural, single vowel sign
    "ූ",    # participle, single vowel sign
    "ාව",   # nominaliser, overlaps with ordinary word-final -āva
})

# Suffixes that belong to the SPOKEN register. Kept in the table because ASR
# transcripts need them, but tagged so a future register-aware scorer can
# weight literary and spoken text against their own baselines instead of
# penalising spoken text for not looking literary.
SPOKEN_REGISTER_SUFFIXES = frozenset({
    "නවා",
    "ලා",     # in its verbal (past participle) reading
})


if __name__ == "__main__":
    # Quick inventory report: python -m quality_pipeline.quality.morphology.suffix_rules
    from collections import Counter

    counts = Counter(cat for _, cat in SUFFIX_TABLE)
    print(f"Total distinct suffixes: {len(SUFFIX_TABLE)}")
    for cat in CATEGORIES:
        print(f"  {cat:<14} {counts[cat]:>3}")
    print(f"\nAmbiguous (multi-category): {len(AMBIGUOUS_SUFFIXES)}")
    for s, cats in sorted(AMBIGUOUS_SUFFIXES.items()):
        print(f"  {s!r:<10} -> {', '.join(cats)}")
    print(f"\nLow-confidence entries: {len(LOW_CONFIDENCE_SUFFIXES)}")
    print(f"Longest suffix: {SUFFIX_TABLE[0][0]!r} ({len(SUFFIX_TABLE[0][0])} codepoints)")