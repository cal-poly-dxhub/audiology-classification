import json
import boto3
import os

s3 = boto3.client("s3")
bucket_name = "dxhub-phii-project-notes"
batch_input_prefix = "mee-batch-inference/input"

input_file = "AbrThr_Data_Redacted.json"
with open(input_file, "r", encoding="utf-8") as file:
    input_json = json.load(file)
# print(input_json)

config_file = "config.json"
with open(config_file, "r", encoding="utf-8") as file:
    config = json.load(file)

institution = "MassEyeAndEar"
institution_template = config["templates"].get(institution, {}).get("template", {})
guidelines = config.get("processing_guidelines", {}).get("rules", [])

if not institution_template:
    raise ValueError(f"Error: No template found for institution '{institution}' in {config_file}")

if not guidelines:
    raise ValueError("Error: No processing guidelines found in config.json")

batch_inputs = []
for idx, patient in enumerate(input_json, start=1):
    report = patient.get("report") or patient.get("Report", "").strip()
    results = patient.get("results") or patient.get("Results", [])

    if not report and not results:
        print(f"Skipping patient {idx}: No report or audiometric results.")
        continue

    record_id = f"PAT{idx:08d}"

    # general prompt, used for CDC and MEE
    # TODO: test with redcap
    model_input = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": (
                    "You are an expert audiologist assistant that extracts explicit hearing test data and classifies hearing loss accurately.\n\n"
                    f"Here is the **hearing report**:\n\n{report}\n\n"
                    f"Here are the **audiometric test results**:\n\n{json.dumps(results, indent=4)}\n\n"
                    "**Use the classification template and guidelines** to determine:\n\n"
                    f"{json.dumps(institution_template, indent=4)}\n\n"
                    f"Guidelines for classification:\n```json\n{json.dumps(guidelines, indent=4)}\n```\n\n"
                    "**Task:**\n"
                    "- Fill in missing values using classification rules.\n"
                    "- Assign the correct 'Better Ear'.\n"
                    "- Provide **reasoning** for each decision.\n"
                    "- Cite **guideline numbers** used.\n"
                    "**Output:**\n"
                    "- Return the JSON **exactly in the given format**, with values populated."
                    "- **DO NOT RETURN any commentary or anything else other than the formatted JSON**"
                )
            }]
        }]
    }

    batch_inputs.append({"recordId": record_id, "modelInput": model_input})

if not batch_inputs:
    raise ValueError("Error: No valid patient data found. Exiting...")

jsonl_filename = "batch_input.jsonl"
with open(jsonl_filename, "w", encoding="utf-8") as jsonl_file:
    for entry in batch_inputs:
        jsonl_file.write(json.dumps(entry) + "\n")

print(f"JSONL file '{jsonl_filename}' created successfully with {len(batch_inputs)} records!")

s3_key = f"{batch_input_prefix}/{jsonl_filename}"
s3.upload_file(jsonl_filename, bucket_name, s3_key, ExtraArgs={"ContentType": "application/json"})

print(f"Uploaded {jsonl_filename} to s3://{bucket_name}/{s3_key}")
