# beachcomb

This tool can help analyze and sort files recovered by file carving. It
accepts a folder with a large number of files that may not have proper modification dates
and filenames. It attempts to classify them by filetype, performs integrity checks,
and regenerates approximate filenames and dates from internal metadata. It’s particularly
useful for datasets extracted with the excellent PhotoRec data recovery tool. Tested on
macOS only.

> **What are "carved files"?**  
> Tools like **PhotoRec** can recover files from a disk with a missing or damaged directory.
> They scan the raw storage for
> known data structures, such as JPEG headers. Usually, metadata such as filenames, folders
> and creation dates is lost. **beachcomb** will attempt
> to classify, validate, date, organize and name the recovered files.

## Features
- Identify popular file types and eliminate exact duplicates.
- Recover plausible dates from EXIF, XMP, IPTC, QuickTime, and internal Office data.
- Sort into bins by filetype and date.
- Validate file integrity and segregate damaged files.
- Optionally generate filenames from internal metadata.
- Add a hidden OCR text layer to PDFs for Spotlight search.
- Generate a human-friendly HTML report.

## Caveat
This is a work in progress. Many features are buggy or incomplete. Use at your own
risk on a backup. Parts of the report are incorrect. Verify your results.

## Quickstart
This probably doesn’t fully work yet.
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

You can also run via Python, if installation was successful:
```bash
python -m beachcomb --help
```

## Installation notes
- Python >=3.9 is required.
- External tools are required and can be installed via Homebrew:

```bash
brew install exiftool ffmpeg qpdf poppler mupdf-tools ghostscript ocrmypdf
```
Many Python modules are required and must be installed with pip install.

## License
MIT © 2025 Chris Ferebee

## Credits & Acknowledgements
Coding by ChatGPT, errors by Chris Ferebee.
This project is not affiliated with the excellent PhotoRec/TestDisk.
Mad props to exiftool, which does a lot of the heavy lifting.

## Contributing
PRs welcome.
