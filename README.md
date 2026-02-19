# Teams Chat Converter

Convert Microsoft Teams chat exports from Purview HTML format to structured Excel spreadsheets with conversation threading, URL extraction, and attachment tracking.

## Features

- ✅ **HTML to Excel Conversion** - Parse Purview Teams chat exports into organized Excel files
- ✅ **Duplicate Detection** - Automatically removes duplicate messages
- ✅ **URL Extraction** - Identifies and extracts all URLs from messages
- ✅ **Attachment Tracking** - Captures attachment names and references
- ✅ **Timestamp Analysis** - Detects conversation gaps and timestamp drifts
- ✅ **Conversation Threading** - Maintains chronological message order
- ✅ **User-Friendly GUI** - Simple browse-and-convert interface
- ✅ **Standalone Executable** - No Python installation required for end users

## Quick Start

### For End Users (Windows)

1. Download `TeamsChartConverter.exe` from the [Releases](https://github.com/Dahicup1032/teams-chat-processor/releases) page
2. Double-click the executable to launch the GUI
3. Click **Browse** to select your Purview HTML export file
4. Click **Convert to Excel**
5. The Excel file will be created in the same folder as your input file

### For Developers

#### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

#### Installation

```bash
# Clone the repository
git clone https://github.com/Dahicup1032/teams-chat-processor.git
cd teams-chat-processor

# Install dependencies
pip install -r requirements.txt