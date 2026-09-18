import os
import re
import json
from pypdf import PdfReader

SOURCE_DIR = "/tmp/de-source-repo"

def extract_from_pdf(filepath):
    reader = PdfReader(filepath)
    full_text = ""
    for idx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        full_text += f"\n--- PAGE {idx+1} ---\n" + text
    return full_text

def analyze_all_files():
    data = {}
    for root, dirs, files in os.walk(SOURCE_DIR):
        if ".git" in root:
            continue
        for f in sorted(files):
            if f.endswith(".pdf") or f.endswith(".txt"):
                path = os.path.join(root, f)
                rel_path = os.path.relpath(path, SOURCE_DIR)
                try:
                    if f.endswith(".pdf"):
                        text = extract_from_pdf(path)
                    else:
                        with open(path, "r", encoding="utf-8", errors="ignore") as tf:
                            text = tf.read()
                    data[rel_path] = {
                        "size_bytes": os.path.getsize(path),
                        "text_length": len(text),
                        "content": text
                    }
                except Exception as e:
                    data[rel_path] = {"error": str(e)}
    return data

if __name__ == "__main__":
    results = analyze_all_files()
    summary = {k: {"size": v.get("size_bytes", 0), "text_len": v.get("text_length", 0)} for k, v in results.items()}
    print(json.dumps(summary, indent=2))
