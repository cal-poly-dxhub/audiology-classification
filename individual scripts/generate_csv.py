import json
import csv
import re

jsonl_filename = "downloaded_results_1744048199.jsonl.out"
csv_filename = "batch_output_redcap_cleaned.csv"
error_log = "json_skipped_log.txt"

# CSV Headers
csv_headers = [
    "Patient Index",
    "Raw Report",
    "Audiometric Test Results",
    "Left Ear Type", "Left Ear Degree",
    "Right Ear Type", "Right Ear Degree",
    "Known Hearing Loss Risk", "Tier One Risk Factors", "Tier Two Risk Factors",
    "Reasoning"
]

# Function to clean control characters from a JSON string
def sanitize_json_line(line):
    # Remove control characters except newline (\n), carriage return (\r), and tab (\t)
    line = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', line)
    return line

with open(csv_filename, mode="w", encoding="utf-8", newline="") as csv_file, \
     open(jsonl_filename, mode="r", encoding="utf-8") as jsonl_file, \
     open(error_log, mode="w", encoding="utf-8") as log_file:

    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(csv_headers)
    patient_index = 1

    for line_number, line in enumerate(jsonl_file, 1):
        try:
            clean_line = sanitize_json_line(line)

            if any(ord(c) > 127 for c in clean_line):
                print(f"⚠️ Warning: Non-ASCII characters detected in line {line_number}")

            record = json.loads(clean_line)
            patient_id = record.get("recordId", f"PAT{str(patient_index).zfill(8)}")

            # Extract text-based sections
            raw_report = "Unknown"
            audiometric_results = "Unknown"
            messages = record.get("modelInput", {}).get("messages", [])

            for msg in messages:
                for content in msg.get("content", []):
                    if content.get("type") == "text":
                        text_data = content.get("text", "")
                        if "**Hearing Report:**" in text_data:
                            try:
                                raw_report = text_data.split("**Hearing Report:**")[1].split("**Audiometric Test Results:**")[0].strip()
                            except IndexError:
                                pass
                        if "**Audiometric Test Results:**" in text_data:
                            try:
                                audiometric_results = text_data.split("**Audiometric Test Results:**")[1].split("**Classification Template:**")[0].strip()
                            except IndexError:
                                pass
                        break

            # Extract and sanitize model output JSON
            model_output_content = record.get("modelOutput", {}).get("content", [])
            attributes_json = {}

            if model_output_content:
                raw_json_text = model_output_content[0].get("text", "").strip()

                json_match = re.search(r"```json\s*(\{.*?\})\s*```", raw_json_text, re.DOTALL)
                if json_match:
                    raw_json_text = json_match.group(1).strip()

                raw_json_text = sanitize_json_line(raw_json_text)

                try:
                    attributes_json = json.loads(raw_json_text)
                except json.JSONDecodeError as e:
                    log_file.write(f"[Line {line_number} - {patient_id}] JSONDecodeError: {e}\n")
                    continue

            # Extract classification data
            hearing_type = attributes_json.get("Attributes", {}).get("Hearing Type", {})
            left_ear = hearing_type.get("Left Ear", {"Type": "", "Degree": ""})
            right_ear = hearing_type.get("Right Ear", {"Type": "", "Degree": ""})

            risk_indicators = attributes_json.get("Attributes", {}).get("Known Hearing Loss Risk Indicators", {})
            known_risk = risk_indicators.get("Known Hearing Loss Risk", "")
            tier_one_risks = ", ".join(risk_indicators.get("Risk Factors", {}).get("Tier One", []))
            tier_two_risks = ", ".join(risk_indicators.get("Risk Factors", {}).get("Tier Two", []))

            reasoning = attributes_json.get("Attributes", {}).get("Reasoning", "")

            row = [
                patient_id,
                raw_report,
                audiometric_results,
                left_ear.get("Type", ""), left_ear.get("Degree", ""),
                right_ear.get("Type", ""), right_ear.get("Degree", ""),
                known_risk, tier_one_risks, tier_two_risks,
                reasoning
            ]
            csv_writer.writerow(row)

        except Exception as e:
            log_file.write(f"[Line {line_number} - PAT{str(patient_index).zfill(8)}] Unexpected Error: {e}\n")

        patient_index += 1

print(f"\n✅ CSV file '{csv_filename}' created successfully.")
print(f"📄 Skipped records and errors written to '{error_log}'.")
