# Teams Chat Processor

A Python tool for converting Microsoft Purview exported Teams chat HTML files into structured Excel spreadsheets.

This tool supports:
- Single-file conversion
- Folder/batch conversion
- GUI interface
- CLI interface
- Executable packaging
- Extraction of conversation-level metadata and message-level chat content

---

## Requirements

Install dependencies before running:

```bash
pip install -r requirements.txt
```

---

## Usage (recommended)

Use `run_converter_test.py` as the primary runner. It accepts command-line arguments so no file editing is needed.

### Convert a folder (combines all HTML files into one Excel workbook)

```bash
python run_converter_test.py path/to/exports
```

### Convert a folder recursively (include sub-folders)

```bash
python run_converter_test.py path/to/exports -r
```

### Specify an output directory

```bash
python run_converter_test.py path/to/exports -o path/to/output
```

### Write one Excel file per HTML file instead of combining

```bash
python run_converter_test.py path/to/exports --no-combine
```

### All options

```
positional arguments:
  input_folder          Path to the folder containing Teams chat HTML files.

optional arguments:
  -o, --output-dir      Directory where output files are written. Defaults to the input folder.
  -r, --recursive       Search for HTML files recursively in sub-folders.
  --no-combine          Write a separate Excel file per HTML file instead of one combined workbook.
```

### Alternative: built-in converter CLI

`teams_chat_converter.py` also has its own `__main__` entrypoint and can be called directly:

```bash
python teams_chat_converter.py path/to/exports
python teams_chat_converter.py path/to/exports -r -o path/to/output
python teams_chat_converter.py path/to/exports --no-combine
```

For most workflows, `run_converter_test.py` is the recommended entry point.

---

## Features

- Converts Purview Teams HTML exports into Excel
- Extracts conversation participants from `chat-data`
- Extracts per-message:
  - Message ID
  - Sender
  - Timestamp
  - Message text
- Preserves URLs found in message text
- Detects attachments when present
- Removes duplicate messages using message hash
- Produces Excel output for review, filtering, and reporting
- Creates a processing log file
- Supports newer Purview message wrapper formats such as:
  - `unknown-direction-message-wrapper`
  - `message-sender`
  - `message-date`
  - `message-text`

---

## Supported HTML Structure

The parser supports Purview HTML exports where:

- Conversation metadata appears first
- Participants are stored in rows containing `td.chat-data`
- Message blocks are wrapped in elements such as:

```html
<label class="unknown-direction-message-wrapper" id="message339">
