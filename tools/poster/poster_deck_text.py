"""Authoritative poster copy and per-run text structure for ``DS_Capstone_Poster_FINAL.pptx``.

**Source of truth:** the hand-saved file ``DS_Capstone_Poster_FINAL.pptx`` (working copy
typically under ``tools/poster/exports/``; see ``tools/poster/README.md``).
``build_poster.py`` uses this module for slide-1 user-facing text.

**No ASCII hyphen (U+002D / ``-``) in on-slide copy:** use an **en dash** (U+2013 / ``–``)
in ``POSTER_DECK_TEXT`` for compound forms and metrics (e.g. ``PR\u2013AUC``). The helper
:func:`ban_ascii_hyphen` maps ``-`` to the en dash when ingesting a deck; hand-edit the module
or the deck to follow the same rule when adding text.

For a **byte-identical** duplicate, run
``python tools/poster/scripts/poster_replicate_byte_identical.py`` —
not ``build_poster.py`` (template rebuilds cannot match a hand file byte-for-byte).

After editing the FINAL in PowerPoint: run
``python tools/poster/scripts/extract_poster_deck_runs.py`` to dump
run structure, merge into ``POSTER_DECK_TEXT`` (and use :func:`ban_ascii_hyphen` on every new
string), apply with :func:`apply_poster_deck_text` to the saved deck, then
``python tools/poster/scripts/verify_poster_deck_against_pptx.py``.
"""

from __future__ import annotations

from typing import List


def ban_ascii_hyphen(s: str) -> str:
    """
    Replace the ASCII hyphen-minus (U+002D) with a typographic en dash (U+2013 / ``–``).
    Use for all poster *copy*; do not strip legitimate minus signs in mathematics here (this deck
    uses none as ``-``+digit).
    """
    return s.replace("\u002d", "\u2013")

POSTER_DECK_TEXT: dict[str, list[list[str]]] = {'TextBox 11': [['Predictive Supply Chain Analytics', ' ', 'for Backorder Risk'],
                ['Temporal holdout: LR (deployable) vs. Stack (rule selected) vs. XGBoost'],
                ['Addy Cruz'],
                ['Advisor: Dr. Liang Jingsai'],
                ['DATA 470 Capstone  |  Data Science  |  Westminster University']],
 'TextBox 17': [['Motivation']],
 'TextBox 18': [['Data']],
 'TextBox 19': [['Models']],
 'TextBox 20': [['Validation & metrics']],
 'TextBox 21': [['Results']],
 'TextBox 22': [['Limits & next steps', ':', ' when to boost']],
 'TextBox 23': [['Complex models can look best on the wrong split. Pick on time, not a leaderboard.'],
                ['Order time features only; post order fields dropped to stay leakage safe.'],
                ['Rare event (~3.4% pos.). Pick the champion on inner temporal CV; report outer holdout once.']],
 'TextBox 24': [['SAP Supply Chain (', 'BigQuery', ' / Kaggle), order line grain.'],
                ['N = 31,177 lines, 3.38% positive (1,054 backorders).'],
                ['13 order time features + 7 missingness flags; 23 post order columns withheld as leaky. '
                 'Bases: LR + LightGBM + RandomForest + kNN. Ensembles: soft vote + OOF calibrated Stack.']],
 'TextBox 25': [['Task'],
                ['Line level backorder classification. Compare LR, XGBoost, and the OOF calibrated Stack on the same temporal protocol.'],
                ['Splits & metrics'],
                ['Train early → test late (temporal). Grouped split is diagnostic only.'],
                ['PR AUC / ROC AUC for ranking. F1 / P / R for the operating point at the frozen OOF threshold.']],
 'TextBox 26': [['What the figures show: time aware splits, feature lineage, LR vs. XGBoost vs. '
                 'Stack', ':']],
 'TextBox 27': [['Temporal is the deploy stress test; grouped is diagnostic.'],
                ['Stack is rule selected champion: ROC 0.936, PR 0.326, F1 0.420. '
                 'LR is the deployable best: ROC 0.910, PR 0.358, F1 0.511, a 40× PR AUC lift over random.'],
                ['Both model gates PASS (precision 0.15, recall 0.35). Overall NO GO comes from label '
                 'maturity (last 180 days: 36% coverage, 32 positives), a dataset condition, not a model defect.'],
                ['Negative results: rolling rate features hurt (label maturity contamination); cascades did not beat LR. '
                 'Ceiling is the dataset, not the modeling layer.']],
 'TextBox 28': [['Limitations'],
                ['Sparse temporal test (n_pos=58) makes thresholded metrics noisy. No post order causal story. Drift makes fixed thresholds fragile.'],
                ['What deployment would require'],
                ['Wait for labels to mature, or shorten the label window. No model change needed.'],
                ['Next'],
                ['Add magnitude / demand / inventory signals at order time. Rolling retrain + drift checks. Viable extension: a second model at a fixed pre outcome clock (e.g. T minus k to ship), same temporal protocol.']],
 'TextBox 2': [['SAP Supply Chain (', 'BigQuery', ' / Kaggle).'],
               ['Reproducible ETL and modeling code accompany the project. LR = penalized '
                'logistic regression; XGB = XGBoost; Stack = OOF calibrated ensemble (selected champion).']],
 'TextBox 3': [['Data Sources:']]}


def _find_shape(slide, name: str):
    for s in slide.shapes:
        if s.name == name:
            return s
    raise KeyError(name)


# Shapes that exist on the hand-finished deck but may be absent on ``Showcase Templates.pptx``.
OPTIONAL_TEXT_SHAPES = frozenset({"TextBox 2", "TextBox 3"})


def assert_shapes_match_pptx(slide, shape_names: List[str] | None = None) -> None:
    """Raise AssertionError with detail if any paragraph/run count or text does not match POSTER_DECK_TEXT.

    The deck structure changed: update ``poster_deck_text.py`` or the .pptx so they match.
    """
    wanted = shape_names or list(POSTER_DECK_TEXT.keys())
    for shname in wanted:
        spec = POSTER_DECK_TEXT[shname]
        try:
            sh = _find_shape(slide, shname)
        except KeyError as e:
            if shname in OPTIONAL_TEXT_SHAPES:
                continue
            raise AssertionError(f"Shape missing on slide: {shname}") from e
        tf = sh.text_frame
        if len(tf.paragraphs) != len(spec):
            raise AssertionError(
                f"{shname}: paragraph count {len(tf.paragraphs)} != spec {len(spec)}"
            )
        for pi, want_runs in enumerate(spec):
            para = tf.paragraphs[pi]
            if len(para.runs) != len(want_runs):
                raise AssertionError(
                    f"{shname} p{pi}: run count {len(para.runs)} != spec {len(want_runs)}"
                )
            for ri, s in enumerate(want_runs):
                got = para.runs[ri].text
                if got != s:
                    raise AssertionError(
                        f"{shname} p{pi} r{ri}: {got!r} != {s!r}"
                    )


def apply_poster_deck_text(slide) -> None:
    """In-place: set every run in ``POSTER_DECK_TEXT``; paragraph/run counts must match the open deck.

    Shapes in ``POSTER_DECK_TEXT`` that are not on the slide are skipped. Any shape that exists
    but disagrees in paragraph/run count raises ``RuntimeError`` (re-save FINAL or update the spec).
    """
    for shname, spec in POSTER_DECK_TEXT.items():
        try:
            sh = _find_shape(slide, shname)
        except KeyError:
            continue
        tf = sh.text_frame
        if len(tf.paragraphs) != len(spec):
            raise RuntimeError(
                f"{shname}: expected {len(spec)} paragraphs, have {len(tf.paragraphs)}. "
                "Update poster_deck_text.py or the .pptx to match."
            )
        for pi, want_runs in enumerate(spec):
            para = tf.paragraphs[pi]
            if len(para.runs) != len(want_runs):
                raise RuntimeError(
                    f"{shname} p{pi}: expected {len(want_runs)} runs, have {len(para.runs)}. "
                    "Update poster_deck_text.py or the .pptx to match."
                )
            for ri, s in enumerate(want_runs):
                para.runs[ri].text = s
