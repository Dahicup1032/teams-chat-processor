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
