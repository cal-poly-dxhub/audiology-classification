import json
import csv

jsonl_filename = "batch_input_redcap.jsonl.out"

csv_filename = "batch_output_redcap.csv"

#TODO: for redcap at the moment, implement abstraction for any form headers
csv_headers = [
    "Patient Index",  
    "Raw Report",
    "Better Ear Type", "Better Ear Degree",
    "Left Ear Type", "Left Ear Degree",
    "Right Ear Type", "Right Ear Degree",
    "Known Hearing Loss Risk", "Tier One Risk Factors", "Tier Two Risk Factors",
    "Reasoning"
]

with open(csv_filename, mode="w", encoding="utf-8", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow(csv_headers)

    with open(jsonl_filename, mode="r", encoding="utf-8") as jsonl_file:
        patient_index = 1

        for line in jsonl_file:
            try:
                record = json.loads(line)

                patient_id = record.get("recordId", f"PAT{str(patient_index).zfill(8)}")  # Fallback if missing

                raw_report = "Unknown"
                messages = record.get("modelInput", {}).get("messages", [])
                for msg in messages:
                    for content in msg.get("content", []):
                        if content.get("type") == "text":
                            text_data = content.get("text", "")
                            if "Here is the hearing report:" in text_data:
                                try:
                                    raw_report = text_data.split("Here is the hearing report:")[1].split("Here are the audiometric test results:")[0].strip()
                                except IndexError:
                                    raw_report = "Unknown"
                            break

                attributes_json = {}
                model_output_content = record.get("modelOutput", {}).get("content", [])

                if model_output_content:
                    raw_json_text = model_output_content[0].get("text", "").strip()

                    # handle Markdown-wrapped JSON (` ```json ... ``` `) -> was causing malformed
                    if raw_json_text.startswith("```json") and raw_json_text.endswith("```"):
                        raw_json_text = raw_json_text[7:-3].strip()

                    try:
                        attributes_json = json.loads(raw_json_text)
                    except json.JSONDecodeError:
                        print(f"Skipping malformed JSON output in record {patient_id}")
                        continue #TODO: figure out why malformed

                hearing_type = attributes_json.get("Attributes", {}).get("Hearing Type", {})
                better_ear = hearing_type.get("Better Ear", {"Type": "", "Degree": ""})
                left_ear = hearing_type.get("Left Ear", {"Type": "", "Degree": ""})
                right_ear = hearing_type.get("Right Ear", {"Type": "", "Degree": ""})

                # extract known hearing loss risk indicators
                risk_indicators = attributes_json.get("Attributes", {}).get("Known Hearing Loss Risk Indicators", {})
                known_risk = risk_indicators.get("Known Hearing Loss Risk", "")
                tier_one_risks = ", ".join(risk_indicators.get("Risk Factors", {}).get("Tier One", [])) if "Risk Factors" in risk_indicators else ""
                tier_two_risks = ", ".join(risk_indicators.get("Risk Factors", {}).get("Tier Two", [])) if "Risk Factors" in risk_indicators else ""

                reasoning = attributes_json.get("Attributes", {}).get("Reasoning", "")

                row = [
                    patient_id,
                    raw_report,
                    better_ear.get("Type", ""), better_ear.get("Degree", ""),
                    left_ear.get("Type", ""), left_ear.get("Degree", ""),
                    right_ear.get("Type", ""), right_ear.get("Degree", ""),
                    known_risk, tier_one_risks, tier_two_risks,
                    reasoning
                ]

                csv_writer.writerow(row)

            except Exception as e:
                print(f"Unexpected Error Processing Record {patient_id}: {e}")

            patient_index += 1

print(f"CSV file '{csv_filename}' created successfully!")
