"""Authoritative poster copy and per-run text structure for ``DS_Capstone_Poster_FINAL.pptx``.

**Source of truth:** the hand-saved file ``DS_Capstone_Poster_FINAL.pptx`` in the repo root.
``build_poster.py`` and ``scripts/apply_poster_text_condensation.py`` use this module for slide-1
user-facing text.

**No ASCII hyphen (U+002D / ``-``) in on-slide copy:** use an **en dash** (U+2013 / ``–``)
in ``POSTER_DECK_TEXT`` for compound forms and metrics (e.g. ``PR\u2013AUC``). The helper
:func:`ban_ascii_hyphen` maps ``-`` to the en dash when ingesting a deck; hand-edit the module
or the deck to follow the same rule when adding text.

For a **byte-identical** duplicate, run ``python scripts/poster_replicate_byte_identical.py`` —
not ``build_poster.py`` (template rebuilds cannot match a hand file byte-for-byte).

After editing the FINAL in PowerPoint: run ``python scripts/extract_poster_deck_runs.py`` to dump
run structure, merge into ``POSTER_DECK_TEXT`` (and use :func:`ban_ascii_hyphen` on every new
string), apply with :func:`apply_poster_deck_text` to the saved deck, then
``python scripts/verify_poster_deck_against_pptx.py``.
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
                ['Temporal holdout: logistic (LR) vs. LightGBM (LGBM)'],
                ['Addy Cruz'],
                ['Advisor: Dr. Liang Jingsai'],
                ['DATA–470 Capstone  |  Data Science  |  Westminster University']],
 'TextBox 17': [['Motivation']],
 'TextBox 18': [['Data']],
 'TextBox 19': [['Models']],
 'TextBox 20': [['Validation & metrics']],
 'TextBox 21': [['Results']],
 'TextBox 22': [['Limits & next steps', ':', ' when to boost']],
 'TextBox 23': [['Imbalance + leakage: complex models can look best on the wrong split—need '
                 'evaluation that matches deployment, not a leaderboard.'],
                ['Order–time (pre–fulfillment) features only; post–order fields '
                 'dropped—leakage–safe, deployable input set.'],
                ['Rare event (~3.4% pos.). Regularized LR vs. LGBM: temporal holdout, not an easy '
                 'random split, is the real test.']],
 'TextBox 24': [['SAP Supply Chain (', 'BigQuery', '/Kaggle): sales … master–order–line.'],
                ['N = 31,177 lines (from 52,118 rows; 59.8% coverage); 3.38% pos. (1,054 '
                 'backorders).'],
                ['13 order–time + 7 missingness; 23 post–order features withheld. Tabular → LR '
                 'first, LGBM as check.']],
 'TextBox 25': [['Task'],
                ['Line–level backorder: penalized LR (default) vs. LGBM. Nonlinear gain must show '
                 'on the temporal test—not only a grouped time–overlap split.'],
                ['Splits & metrics'],
                ['Primary: temporal (train early → test late). Secondary: grouped by document. '
                 'Threshold: 5–fold stratified OOF on train.'],
                ['PR–AUC / ROC–AUC: rank quality. F1, P, R, confusion: rare–event operating '
                 'point. Low temporal F1 with strong ROC is possible.']],
 'TextBox 26': [['What the figures show: time–aware splits, feature lineage, LR vs. LGBM', ':']],
 'TextBox 27': [['If train/test periods overlap, grouped validation is easy; temporal is the '
                 'deploy stress test.'],
                ['Temporal: PR–AUC 0.19 (LR) vs. 0.08 (LGBM); ROC–AUC ~0.85 each—no LGBM ranking '
                 'edge.'],
                ['Ship LR; use LGBM as a complexity benchmark only.'],
                ['Turning off post–order fields cuts grouped F1 vs. a leaky view—mostly '
                 'split/leakage, not model family. CMs/heatmap: recall at one threshold; very few '
                 'positives.']],
 'TextBox 28': [['Limitations:'],
                ['Temporal test: only 0.89% pos. (n=58)—F1/precision at a threshold is noisy. No '
                 'post–order features → no causal story. Drift/label noise: fixed thresholds are '
                 'fragile.'],
                ['If LGBM is worth it'],
                ['Only if temporal PR–AUC clearly beats LR (not just on grouped), or business '
                 'needs recall. Else LR default.'],
                ['Next'],
                ['Magnitude target; entity/demand/inventory signals at order time; rolling '
                 'retrain / drift checks. Viable extension (not a replacement for this order–time '
                 'work): a second model at a fixed pre–outcome clock (e.g. T–k to ship) using only '
                 'fields knowable then (e.g. confirmed quantity, inventory / MRP style inputs), '
                 'evaluated with the same temporal train vs test rule to stay leak–safe.']],
 'TextBox 2': [['SAP Supply Chain (', 'BigQuery', ' / Kaggle).'],
               ['Reproducible ETL and modeling code accompany the project. LR = penalized '
                'logistic regression; LGBM = LightGBM.']],
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
