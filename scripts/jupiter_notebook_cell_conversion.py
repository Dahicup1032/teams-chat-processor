from pathlib import Path
from teams_chat_converter import TeamsChatConverter

# ---- SET YOUR TEST FILE PATH HERE ----
html_path = Path(r"C:\path\to\your\test_export.html")

# Optional: output directory (None = same folder as input)
output_dir = None
# output_dir = r"C:\path\to\output_folder"

# ---- RUN CONVERSION ----
converter = TeamsChatConverter(str(html_path), output_dir)

df = converter.parse_html()
df = converter.remove_duplicates(df)
df = converter.check_timestamp_drift(df)

excel_file = converter.save_to_excel(df)

print("Conversion Complete")
print("Excel file created at:", excel_file)

# Optional stats (if your class tracks them)
if hasattr(converter, "stats"):
    print("Stats:", converter.stats)
