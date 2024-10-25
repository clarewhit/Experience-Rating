import sys
import os
import unicodedata
def we_are_frozen():
    # All of the modules are built-in to the interpreter, e.g., by py2exe
    return hasattr(sys, "frozen")

def module_path():
    encoding = sys.getfilesystemencoding()
    if we_are_frozen():
        print()
        return os.path.dirname(sys.executable.decode(encoding))
    return os.path.dirname(__file__)