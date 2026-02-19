# Purview Teams Chat Converter

Convert Microsoft Purview Teams Chat exports from HTML to Excel with data cleaning, validation, URL extraction, and attachment tracking.

## Features

✅ **Drag-and-Drop GUI** - Simple interface for non-technical users  
✅ **Duplicate Removal** - Automatically detects and removes duplicate messages  
✅ **Timestamp Drift Detection** - Identifies messages out of chronological order  
✅ **URL Extraction** - Captures all URLs with classification (SharePoint, Teams, OneDrive, etc.)  
✅ **Attachment Tracking** - Extracts attachment filenames, types, sizes, and download links  
✅ **Comprehensive Logging** - Detailed processing logs for auditing  
✅ **Excel Output** - Clean, formatted Excel reports with wrapped text  
✅ **Windows 11 Compatible** - Standalone executable, no Python required  

## Installation

### For Developers

1. Install Python 3.9 or higher
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python teams_chat_converter_gui.py
```

### For End Users (Windows 11)

1. Download TeamsChartConverter.exe
2. Double-click to run
3. Drag and drop your HTML file or click Browse

## Quick Start

```bash
git clone https://github.com/Dahicup1032/teams-chat-processor.git
cd teams-chat-processor
pip install -r requirements.txt
python teams_chat_converter_gui.py
```

## License

Internal use only.