import re 
from typing import Optional


title_patterns= re.compile(
    r"""(?ix)                  # i = ignore-case, x = free-spacing
    ^\s*                       # optional leading white-space
    (?:                        # … followed by one of these phrases
        title\s*:              |   #  Title:
        the\s+title\s+is\s*:   |   #  The title is:
        exact\s+title\s+of\s+the\s+paper\s+is\s*: |  #  Exact title of the paper is:
        here\s+is\s+the\s+title\s*:                 #  Here is the title:
    )
    \s*                        # toss any white-space after the label
    """,
    re.UNICODE,
)            




def remove_newlines(text: str) -> str:
        """
        Removes all newline characters from the input string.

        Args:
            text (str): The input string.

        Returns:
            str: The string with all '\n' characters removed.
        """
        return text.replace("\n", "")
    
    
    
def clean_title(raw: str) -> Optional[str]:
    """
    Remove leading label phrases & any stray ':' or ';'.
    Returns the cleaned title, or *None* if nothing is left.
    """
    if raw is None:
        return None

    # 1) strip the label if present
    text = title_patterns.sub("", raw)

    # 2) strip leading / trailing colons, semicolons & white-space
    text = text.strip(" :;\n\t\r")

    # 3) collapse internal runs of white-space to a single space
    text = re.sub(r"\s+", " ", text).strip()

    return text or None   # return None when only labels were present