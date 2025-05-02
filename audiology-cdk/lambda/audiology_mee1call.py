import json
import os
import re
import boto3
import logging
from langchain_aws.chat_models import ChatBedrock
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
logger = logging.getLogger()
logger.setLevel(logging.INFO)

with open("config.json", "r") as file:
    config = json.load(file)

bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-west-2")

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def categorize_diagnosis_with_lm(report, results, institution_template, valid_values, guidelines):
    """Uses LLM to extract explicit facts and classify hearing loss in one step."""

    model_id = "us.amazon.nova-pro-v1:0"
    model_kwargs = {
        "max_tokens": 4096,
        "temperature": 0.0,
        "top_k": 250,
        "top_p": 0.9,
        "stop_sequences": ["\n\nHuman"],
        "inference_profile_arn": "arn:aws:bedrock:us-west-2:762233745628:inference-profile/us.amazon.nova-pro-v1:0"
    }

    model = ChatBedrock(client=bedrock_runtime, model_id=model_id, model_kwargs=model_kwargs)

    results_json_str = json.dumps(results, indent=4).replace("{", "{{").replace("}", "}}")
    json_template_fixed = json.dumps(institution_template, indent=4).replace("{", "{{").replace("}", "}}")

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an expert **pediatric** audiologist that extracts explicit hearing test data and classifies hearing loss accurately."),
        ("human", "{report_text}\n\n"
                "Here are the **audiometric test results**:\n\n{results_json}\n\n"
                "**Use the classification template and guidelines** to determine:\n"
                "{json_template}\n\n"
                "**Valid Values:**\n```json\n{valid_values}\n```\n\n"
                "**Guidelines for Classification:**\n```json\n{guidelines}\n```\n\n"

                "**Processing Rules (MUST Follow):**\n"
                "- **Use only explicitly provided threshold values**; do not infer missing values.\n"
                "- **If multiple severities are listed, assign the most severe classification.**\n"

                "**Output Requirements:**\n"
                "- Return classification in **EXACT JSON format** as per the template, with no modifications.\n"
                "- Provide **precise reasoning** for each classification.\n"
                "- Make sure there is thorough, chain of thought reasoning for each attribute's output."
                 "- **Cite guideline numbers** when making classification decisions.\n"
                "- **DO NOT include any additional explanations, assumptions, or commentary.**\n"
        )
    ])

    chain = prompt | model | StrOutputParser()

    try:
        return chain.invoke({
            "report_text": f"Here is the **hearing report**:\n\n{report}",
            "results_json": results_json_str,
            "json_template": json_template_fixed,
            "valid_values": json.dumps(valid_values, indent=4),
            "guidelines": json.dumps(guidelines, indent=4)
        })
    except Exception as e:
        return f"Error categorizing diagnosis: {e}"

def process_audiology_data(input_json, institution):
    """Processes the audiology JSON data, merging extraction and classification into one step."""

    institution_data = config["templates"].get(institution, {})
    institution_template = institution_data.get("template", {})
    valid_values = institution_data.get("valid_values", {})
    processing_guidelines = institution_data.get("processing_rules", {}).get("rules", [])

    if not institution_template:
        print(f"Error: No template found for institution '{institution}'. Exiting...")
        return

    if not processing_guidelines:
        print(f"Warning: No processing guidelines found for '{institution}', proceeding without them.")

    for index, patient in enumerate(input_json, start=1):
        output_file = os.path.join(OUTPUT_DIR, f"diagnosis_results_{index}_{institution}.json")

        # Skip already processed files
        if os.path.exists(output_file):
            print(f"Skipping patient {index} - already processed.")
            continue

        print(f"\nProcessing patient {index}...\n")

        raw_report = patient.get("Report", "").strip()
        audiometric_results = patient.get("Results", [])

        if not raw_report and not audiometric_results:
            print(f"Skipping patient {index}: No data found.")
            continue

        # Extract and categorize in a single step
        diagnosis_results = categorize_diagnosis_with_lm(raw_report, audiometric_results, institution_template, valid_values, processing_guidelines)
        if "Error" in diagnosis_results:
            print(f"Skipping patient {index} due to categorization error: {diagnosis_results}")
            continue

        print("Diagnosis Categorization Results:\n", diagnosis_results)

        # Extract JSON response
        try:
            match = re.search(r'```json\n(.*?)\n```', diagnosis_results, re.DOTALL)
            if match:
                diagnosis_json_str = match.group(1).strip()
                diagnosis_json = json.loads(diagnosis_json_str)

                # Save each patient's diagnosis separately
                with open(output_file, "w") as json_file:
                    json.dump(diagnosis_json, json_file, indent=4)

                print(f"Diagnosis results saved successfully to {output_file}")

            else:
                print(f"Error: Could not find valid JSON in LLM response for patient {index}.")

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON for patient {index}: {e}")

def lambda_handler(event, context):
    # Log the received event
    logger.info("Received event: %s", json.dumps(event))

    name = event.get("queryStringParameters", {}) .get("name", "world")

    # Build a response
    response = {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Hello, {name}!"
        })
    }
    return response

if __name__ == "__main__":
    input_file_path = "AbrThr_Data_Redacted.json"
    institution = "Redcap"

    with open(input_file_path, "r", encoding="utf-8") as input_file:
        input_json = json.load(input_file)

    process_audiology_data(input_json, institution)
