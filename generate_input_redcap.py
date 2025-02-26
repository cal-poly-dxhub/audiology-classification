import json
import boto3
import os

s3 = boto3.client("s3")
bucket_name = "dxhub-phii-project-notes"
batch_input_prefix = "mee-batch-inference/input"

input_file = "AbrThr_Data_Redacted.json"
with open(input_file, "r", encoding="utf-8") as file:
    input_json = json.load(file)

config_file = "config.json"
with open(config_file, "r", encoding="utf-8") as file:
    config = json.load(file)

institution = "Redcap" #using redcap
redcap_template = config["templates"].get(institution, {}).get("template", {})
redcap_guidelines = config.get("redcap_guidelines", {})

if not redcap_template:
    raise ValueError(f"Error: No template found for institution '{institution}' in {config_file}")

if not redcap_guidelines:
    raise ValueError("Error: No processing guidelines found for Redcap in config.json")

valid_types = redcap_guidelines["HearingType"]["TypeOfLoss"]
valid_degrees = redcap_guidelines["HearingType"]["DegreeOfLoss"]
valid_risk_options = redcap_guidelines["KnownHearingLossRisk"]["Options"]
tier_one_risks = redcap_guidelines["KnownHearingLossRisk"]["TierOneRiskFactors"]
tier_two_risks = redcap_guidelines["KnownHearingLossRisk"]["TierTwoRiskFactors"]

batch_inputs = []
for idx, patient in enumerate(input_json, start=1):
    report = patient.get("report") or patient.get("Report", "").strip()
    results = patient.get("results") or patient.get("Results", [])

    if not report and not results:
        print(f"Skipping patient {idx}: No report or audiometric results.")
        continue

    # generate an 11-character alphanumeric `recordId`
    record_id = f"PAT{idx:08d}"

    # prompt for Claude -> SPECIFIED FOR REDCAP FORMAT, tweak for prompt to work w/ abstraction
    #TODO: tweak the logic engine and prompt
    model_input = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": (
                    "You are an expert audiologist assistant that extracts explicit hearing test data and classifies hearing loss accurately.\n\n"
                    "**Here is the hearing report:**\n\n"
                    f"{report}\n\n"
                    "**Here are the audiometric test results:**\n\n"
                    f"{json.dumps(results, indent=4)}\n\n"
                    "**Use the classification template and guidelines** to determine:\n\n"
                    f"```json\n{json.dumps(redcap_template, indent=4)}\n```\n\n"
                    "**Hearing Type Options:**\n"
                    f"- Type: {json.dumps(valid_types, indent=4)}\n"
                    f"- Degree: {json.dumps(valid_degrees, indent=4)}\n\n"
                    "**Known Hearing Loss Risk Indicators:**\n"
                    f"- Options: {json.dumps(valid_risk_options, indent=4)}\n"
                    f"- Tier One: {json.dumps(tier_one_risks, indent=4)}\n"
                    f"- Tier Two: {json.dumps(tier_two_risks, indent=4)}\n\n"
                    "**Task:**\n"
                    "- Fill in missing values using classification rules.\n"
                    "- Assign the correct 'Better Ear' based on hearing loss severity.\n"
                    "- Use only valid options listed above (strict validation).\n"
                    "- Provide **reasoning** for each classification decision.\n"
                    "- Cite **guideline numbers** used in decisions.\n"
                    "**Output:**\n"
                    "- Return the JSON **exactly in the given format**, with values populated.\n"
                    "- Ensure a `Reasoning` section is added explaining classification decisions.\n"
                    "- **DO NOT RETURN any commentary or anything else other than the formatted JSON**\n\n"
                    "**Example Output Format:**\n"
                    "```json\n"
                    "{\n"
                    "    \"formtype\": \"Redcap\",\n"
                    "    \"Attributes\": {\n"
                    "        \"Hearing Type\": {\n"
                    "            \"Better Ear\": {\n"
                    "                \"Type\": \"Sensorineural\",\n"
                    "                \"Degree\": \"Moderate (41-55 dB HL)\"\n"
                    "            },\n"
                    "            \"Left Ear\": {\n"
                    "                \"Type\": \"Sensorineural\",\n"
                    "                \"Degree\": \"Moderate (41-55 dB HL)\"\n"
                    "            },\n"
                    "            \"Right Ear\": {\n"
                    "                \"Type\": \"Sensorineural\",\n"
                    "                \"Degree\": \"Moderate (41-55 dB HL)\"\n"
                    "            }\n"
                    "        },\n"
                    "        \"Known Hearing Loss Risk Indicators\": {\n"
                    "            \"Known Hearing Loss Risk\": \"Yes\",\n"
                    "            \"Risk Factors\": {\n"
                    "                \"Tier One\": [\"Down syndrome\", \"Bacterial meningitis\"],\n"
                    "                \"Tier Two\": [\"Speech/Language delay\"]\n"
                    "            }\n"
                    "        },\n"
                    "        \"Reasoning\": \"Patient has moderate sensorineural hearing loss based on threshold levels. Left and Right ears both show similar loss, so either ear could be the 'Better Ear'.\n"
                    "        Bacterial meningitis and Down syndrome are listed as risk factors (Guidelines 3, 7).\"\n"
                    "    }\n"
                    "}\n"
                    "```"
                )
            }]
        }]
    }

    batch_inputs.append({"recordId": record_id, "modelInput": model_input})

if not batch_inputs:
    raise ValueError("Error: No valid patient data found. Exiting...")

jsonl_filename = "batch_input_redcap.jsonl"
with open(jsonl_filename, "w", encoding="utf-8") as jsonl_file:
    for entry in batch_inputs:
        jsonl_file.write(json.dumps(entry) + "\n")

print(f"JSONL file '{jsonl_filename}' created successfully with {len(batch_inputs)} records!")

s3_key = f"{batch_input_prefix}/{jsonl_filename}"
s3.upload_file(jsonl_filename, bucket_name, s3_key, ExtraArgs={"ContentType": "application/json"})

print(f"Uploaded {jsonl_filename} to s3://{bucket_name}/{s3_key}")
