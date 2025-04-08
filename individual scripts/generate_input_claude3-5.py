import json
import boto3
import os

s3 = boto3.client("s3")
input_bucket = "dxhub-phii-project-notes"
input_prefix = "meei-deidentidfied-data-raw/"
output_prefix = "mee-batch-inference/input/"
local_output_dir = "batch_inputs"

os.makedirs(local_output_dir, exist_ok=True)

# Load config with templates and processing guidelines
config_file = "config.json"
with open(config_file, "r", encoding="utf-8") as file:
    config = json.load(file)

institution = "Redcap" # Options: "MassEyeAndEar", "CDC", "Redcap", "Dawn"
institution_data = config["templates"].get(institution, {})

# Validate template existence
institution_template = institution_data.get("template", {})
valid_values = institution_data.get("valid_values", {})
processing_guidelines = institution_data.get("processing_rules", {}).get("rules", [])

if not institution_template:
    raise ValueError(f"Error: No template found for institution '{institution}' in {config_file}")

if not processing_guidelines:
    print(f"Warning: No processing guidelines found for '{institution}', proceeding without them.")

response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
input_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

if not input_files:
    raise ValueError("Error: No JSON files found in input bucket.")

print(f"Found {len(input_files)} input files in S3 bucket '{input_bucket}'.")

# Process each JSON file
for file_key in input_files:
    print(f"Downloading and processing file: {file_key}")

    file_obj = s3.get_object(Bucket=input_bucket, Key=file_key)
    input_json = json.loads(file_obj["Body"].read().decode("utf-8"))

    batch_inputs = []
    for idx, patient in enumerate(input_json, start=1):
        report = patient.get("report") or patient.get("Report", "").strip()
        results = patient.get("results") or patient.get("Results", [])

        if not report and not results:
            print(f"Skipping patient {idx} in {file_key}: No report or audiometric results.")
            continue

        record_id = f"PAT{idx:08d}"

        # Generate model input dynamically based on institution template
        model_input = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": (
                        "You are an expert **pediatric** audiologist assistant responsible for extracting explicit hearing test data and classifying hearing loss with precision."
                        " Your classification must strictly follow given templates and clinical guidelines.\n\n"

                        "**Hearing Report:**\n\n"
                        f"{report}\n\n"

                        "**Audiometric Test Results:**\n\n"
                        f"{json.dumps(results, indent=4)}\n\n"

                        "**Classification Template:**\n\n"
                        f"{json.dumps(institution_template, indent=4)}\n\n"

                        "**Valid Values:**\n"
                        f"```json\n{json.dumps(valid_values, indent=4)}\n```\n\n"

                        "**Classification Guidelines (MUST FOLLOW):**\n"
                        f"```json\n{json.dumps(processing_guidelines, indent=4)}\n```\n\n"

                        "**Processing Rules (MUST Follow):**\n"
                        "- **Use only explicitly provided threshold values**; do not infer missing values.\n"
                        "- **If multiple severities are listed, assign the most severe classification.**\n"

                        "**Output Requirements:**\n"
                        "- Fill in missing values using classification rules.\n"
                        "- Assign the correct 'Better Ear' based on hearing loss severity.\n"
                        "- Use only valid options listed above (strict validation).\n"
                        "- Provide **reasoning** for each classification decision.\n"
                        "- Cite **guidelines** used in decisions.\n"
                        
                        "- Return classification in **EXACT JSON format** as per the template, with no modifications.\n"
                        "- Provide **precise reasoning** for each classification.\n"
                        "- Make sure there is a **detailed, thorough, chain of thought reasoning for each attribute's output** and how it came to that conclusion."
                        "- Reasoning must include reasoning for the left ear, right ear, and risk factors"
                        "- **Cite guideline numbers** when making classification decisions.\n"
                        "- **DO NOT include any additional explanations, assumptions, or commentary.**\n"
                    )
                }]
            }]
        }

        batch_inputs.append({"recordId": record_id, "modelInput": model_input})

    if not batch_inputs:
        print(f"No valid patient data found in {file_key}, skipping output generation.")
        continue

    input_filename = file_key.split("/")[-1].replace(".json", f"_{institution.lower()}_batch.jsonl")
    local_jsonl_path = os.path.join(local_output_dir, input_filename)

    with open(local_jsonl_path, "w", encoding="utf-8") as jsonl_file:
        for entry in batch_inputs:
            jsonl_file.write(json.dumps(entry) + "\n")

    print(f"Saved JSONL file locally: {local_jsonl_path} with {len(batch_inputs)} records.")

    output_s3_key = f"{output_prefix}{input_filename}"
    s3.upload_file(local_jsonl_path, input_bucket, output_s3_key, ExtraArgs={"ContentType": "application/json"})

    print(f"Uploaded {local_jsonl_path} to s3://{input_bucket}/{output_s3_key}")

print("All input files processed, saved locally, and uploaded successfully!")
