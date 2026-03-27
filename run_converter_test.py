"""
VS Code Runner for Teams Chat Converter

"""

from teams_chat_converter import convert_teams_chat_folder

INPUT_FOLDER = r"C:\Users\tonyn\Desktop\Purview_Practice_Export\Final Test\input"
OUTPUT_FOLDER = None

if __name__ == "__main__":
    print("\n🚀 Starting Teams Chat Conversion...\n")
    print(f"Using input folder: {INPUT_FOLDER}\n")

    try:
        excel_file, log_file = convert_teams_chat_folder(
            INPUT_FOLDER,
            OUTPUT_FOLDER
        )

        print("\n✅ SUCCESS")
        print(f"Excel Output: {excel_file}")
        print(f"Log File: {log_file}")

    except Exception as e:
        print("\n❌ ERROR OCCURRED")
        print(str(e))