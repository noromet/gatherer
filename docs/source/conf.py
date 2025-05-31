import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

project = "gatherer"
copyright = "2025, Juan Torrente"
author = "Juan Torrente"
release = "1.3"
version = "1.3"

today_fmt = "%Y-%m-%d"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # for Google/Numpy-style docstrings
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx.ext.todo",
    "sphinx.ext.mathjax",
    "rst2pdf.pdfbuilder",  # for PDF generation
]


napoleon_google_docstring = True
napoleon_numpy_docstring = True

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

pdf_documents = [("index", "GathererDocs", "Gatherer Documentation", "Juan Torrente")]
