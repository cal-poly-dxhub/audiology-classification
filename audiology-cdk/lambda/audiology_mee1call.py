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
s3_client = boto3.client("s3")

bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-west-2")
BUCKET_NAME = os.environ['BUCKET_NAME']


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
        ("system",
         "You are an expert **pediatric** audiologist that extracts explicit hearing test data and classifies hearing loss accurately."),
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

    # load config from S3
    resp = s3_client.get_object(Bucket=BUCKET_NAME, Key="/Config/config.json")
    body = resp['Body'].read()
    config = json.loads(body)

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
        print(f"\nProcessing patient {index}...\n")

        raw_report = patient.get("Report", "").strip()
        audiometric_results = patient.get("Results", [])

        if not raw_report and not audiometric_results:
            print(f"Skipping patient {index}: No data found.")
            continue

        # Extract and categorize in a single step
        diagnosis_results = categorize_diagnosis_with_lm(raw_report, audiometric_results, institution_template,
                                                         valid_values, processing_guidelines)
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

                # TODO
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key="<provider>_lab_output/<patient>",
                    Body=diagnosis_json,
                    ContentType='application/json'
                )
                print(f"Diagnosis results saved successfully to {output_file}")

            else:
                print(f"Error: Could not find valid JSON in LLM response for patient {index}.")

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON for patient {index}: {e}")


def lambda_handler(event, context):
    # Log the received event
    logger.info("Received event: %s", json.dumps(event))

    # Obtain Patient Record from S3
    record = event.get("Records", [])
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    resp = s3_client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()

    try:
        data = json.loads(body)
        process_audiology_data(data, "Redcap")  # TODO Change where the institution field comes from

    except json.JSONDecodeError:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error parsing record: {record}"
            })
        }
    response = {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Successfully Processed patient record: {record}"
        })
    }
    return response
