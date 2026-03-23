"""Sphinx configuration for the stagegate documentation."""

from __future__ import annotations

import sys
from pathlib import Path

import sphinx_rtd_theme

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

project = "stagegate"
author = "ttkkmg"
copyright = "2026, ttkkmg"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
]

templates_path = ["_templates"]
exclude_patterns: list[str] = []

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"
language = "en"

autosummary_generate = True
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "imported-members": True,
    "exclude-members": "WAIT_CONDITIONS",
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False

myst_heading_anchors = 3

html_theme = "sphinx_rtd_theme"
html_static_path: list[str] = []

html_context = {
    "display_github": True,
    "github_user": "ttkkmg",
    "github_repo": "stagegate",
    "github_version": "main",
    "conf_py_path": "/docs/source/",
}
