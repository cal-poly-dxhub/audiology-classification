import json
import os
import time
import boto3
import re
from langchain_aws.chat_models import ChatBedrock
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from PyPDF2 import PdfReader
from docx import Document

with open("config.json", "r") as file:
    config = json.load(file)

bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

def read_pdf(file_path):
    try:
        reader = PdfReader(file_path)
        return "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return None

def read_docx(file_path):
    try:
        doc = Document(file_path)
        return "\n".join([para.text for para in doc.paragraphs])
    except Exception as e:
        print(f"Error reading DOCX: {e}")
        return None

def extract_explicit_facts(raw_report):
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

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a precise audiology assistant that extracts explicit information from hearing reports."),
        ("human", f"Here is a raw hearing report:\n\n{raw_report}\n\n"
                  "Extract and categorize explicit facts under:\n"
                  "- Reason for Test\n"
                  "- History\n"
                  "- Hearing Measurements (Left Ear and Right Ear details)\n"
                  "- Interpretation (specific hearing types and severities per ear)\n"
                  "- Recommendations\n"
                  "Return **only explicitly stated facts** without assumptions."),
    ])

    chain = prompt | model | StrOutputParser()

    try:
        return chain.invoke(input={})
    except Exception as e:
        return f"Error extracting explicit facts: {e}"

def categorize_diagnosis_with_lm(facts, institution_template, guidelines):
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

    # inject JSON structure as a template for the LLM to populate
    json_template_fixed = json.dumps(institution_template, indent=4).replace('{', '{{').replace('}', '}}')
    classification_rules_fixed = json.dumps(config['mass_eye_and_ear_rules'], indent=4).replace('{', '{{').replace('}', '}}')

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an expert audiologist who strictly follows hearing loss diagnosis guidelines."),
        ("human", f"Here are the **explicit** facts extracted from the hearing report:\n\n{facts}\n\n"
                  f"Here is the structured **template** for classification:\n\n{json_template_fixed}\n\n"
                #   f"Here are the **classification rules**:\n\n```json\n{classification_rules_fixed}\n```\n\n"
                  f"Here are the **guidelines** for hearing loss classification:\n\n```json\n{json.dumps(guidelines, indent=4)}\n```\n\n"
                  "**Task:**\n"
                  "- Fill in the missing values in the JSON structure using the classification rules.\n"
                  "- Follow the provided guidelines strictly.\n"
                  "Ensure that the 'Better Ear' matches either the Left Ear or Right Ear, based on the least degree of loss."
                  "- Provide **chain-of-thought reasoning** explaining each classification.\n"
                  "- Cite the **guideline number** used for each decision.\n"
                  "**Output:**\n"
                  "- Return the JSON **exactly in the given format**, with missing values correctly populated."),
    ])

    chain = prompt | model | StrOutputParser()

    try:
        return chain.invoke(input={})
    except Exception as e:
        return f"Error categorizing diagnosis: {e}"

def main():
    file_path = "Copy of Deborah Dragon Fish (1).docx" #harcoded for now, need to implement mass diagnosis w s3
    institution = "MassEyeAndEar" #can change to an institution in the config (CDC, MassEyeAndEar, Example)

    if file_path.lower().endswith(".pdf"):
        raw_report = read_pdf(file_path)
    elif file_path.lower().endswith(".docx"):
        raw_report = read_docx(file_path)
    else:
        print("Unsupported file format. Please provide a PDF or DOCX file.")
        return

    if not raw_report:
        print("Failed to extract text from the file")
        return

    print("Step 1: Extract Explicit Facts")
    explicit_facts = extract_explicit_facts(raw_report)
    print("Explicit Facts Extracted:")
    print(explicit_facts)

    time.sleep(2)

    # fetch the institution's output template from config.json
    institution_template = config["templates"].get(institution, {}).get("template", {})
    if not institution_template:
        print(f"Error: No template found for institution '{institution}'. Exiting...")
        return
    # print(institution_template)

    guidelines = config.get("processing_guidelines", {}).get("rules", [])
    if not guidelines:
        print("Error: No processing guidelines found in config.json")
        return

    print("\nStep 2: Categorize Diagnosis Using LLM")
    diagnosis_results = categorize_diagnosis_with_lm(explicit_facts, institution_template, guidelines)
    print("Diagnosis Categorization Results:")
    print(diagnosis_results)
    try:
        match = re.search(r'```json\n(.*?)\n```', diagnosis_results, re.DOTALL)
        if match:
            diagnosis_json_str = match.group(1).strip()
            diagnosis_json = json.loads(diagnosis_json_str)

            output_file = f"diagnosis_results_{file_path}.json"
            with open(output_file, "w") as json_file:
                json.dump(diagnosis_json, json_file, indent=4)

            print(f"Diagnosis results saved successfully to {output_file}")
        else:
            print("Error: Could not find valid JSON in LLM response.")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        

if __name__ == "__main__":
    main()