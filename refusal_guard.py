REFUSAL_PHRASES = [
    "i'm sorry", "i am sorry", "i don't have", "i do not have",
    "i cannot", "i can't", "i apologize", "not enough information",
    "insufficient information", "unable to create", "unable to generate",
    "i'm unable", "i am unable", "as an ai", "as a language model",
    "understood", "no caption will be", "cannot provide", "can't provide",
    "i won't", "i will not", "here is", "here's the caption",
]

def is_refusal(text):
    """Prefix match — use where you're checking a single clean output
    (a generated caption, a generated hook) right after generation."""
    if not text:
        return False
    return any(text.strip().lower().startswith(p) for p in REFUSAL_PHRASES)

def contains_refusal(text):
    """Substring match — use at a publish boundary on a multi-line blob
    where the refusal might not be the first thing in the file."""
    if not text:
        return False
    low = text.lower()
    return any(p in low for p in REFUSAL_PHRASES)