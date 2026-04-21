"""Shared layout constants for [build_poster.py](build_poster.py) and [scripts/verify_poster_layout.py](scripts/verify_poster_layout.py).

Keep in sync when changing section/body pairing or minimum gaps."""

# EMU: 914400 per inch. Gap between section heading box and its body text box.
HEAD_BODY_GAP_EMU = 110000

# (heading TextBox id, body TextBox id) — Westminster template slide 1
HEAD_BODY_PAIRS = (
    ("TextBox 17", "TextBox 23"),
    ("TextBox 18", "TextBox 24"),
    ("TextBox 19", "TextBox 25"),
    ("TextBox 20", "TextBox 26"),
    ("TextBox 21", "TextBox 27"),
    ("TextBox 22", "TextBox 28"),
)

# Minimum acceptable body height after nudge (build fails if lower)
MIN_BODY_HEIGHT_EMU = 800000

# Allowed slack when verifying (OOXML rounding)
LAYOUT_VERIFY_TOLERANCE_EMU = 50000
