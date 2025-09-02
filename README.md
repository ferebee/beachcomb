# beachcomb

Analyze & sort carved files by filetype and date with dedup, integrity checks and
reporting. Tested on macOS only.

> **What are "carved files"?**  
> Tools like **PhotoRec** can recover files from a disk with a missing or damaged directory
> structure with “file carving”. They scan the raw storage for
> known file data structures, such as JPEG headers. Usually, metadata such as filenames
> and creation dates is lost along with the directory structure. **beachcomb** will attempt
> to classify, validate, date, organize and name the recovered files.

## Features
- Identify and popular file types, eliminate exact duplicates.
- Recover plausible dates (EXIF, XMP, IPTC, QuickTime, Office).
- Sort into bins by filetype and date.
- Validate file integrity and separate damaged files.
- Optionally generate filenames from internal metadata.
- Add a hidden OCR text layer to PDFs for Spotligh search.
- Generate a human-friendly HTML report.

## Quickstart

```bash
# Recommended for end users:
pipx install beachcomb

# Or inside a virtual environment:
pip install beachcomb
```

Run:

```bash
beachcomb --help
beachcomb --version
# Example:
beachcomb --source /path/to/carved --dest /path/to/sorted --dry-run
```

You can also run via Python:
```bash
python -m beachcomb --help
```

## Installation notes
- Python >=3.9 is required.
- External tools are required and can be installed via Homebrew:

```bash
brew install exiftool ffmpeg qpdf poppler mupdf-tools ghostscript ocrmypdf
```

## License
MIT © 2025 Chris Ferebee

## Credits & Acknowledgements
Concept by Chris Ferebee, execution mostly by LLMs.  
This project is not affiliated with PhotoRec/TestDisk.

## Contributing
PRs welcome.
