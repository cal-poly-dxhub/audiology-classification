import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
import boto3
from botocore.exceptions import ClientError
import re
import csv
import ast

# Import your existing bedrock module
# from bedrock import client, bedrock, llm_model_id, invoke_llm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

start_time = time.time()

class BedrockBatch:
    """Batch processing for AWS Bedrock using existing credentials."""

    
    def __init__(self, region='us-west-2'):
        """Initialize using the profile credentials."""
        self.region = region
    
        # Create a session using the specified profile
        session = boto3.Session(region_name=region)
        # credentials = session.get_credentials().get_frozen_credentials()
            
        # Use the session to create clients
        self.bedrock_client = session.client('bedrock')
        self.bedrock_runtime_client = session.client('bedrock-runtime')
        self.s3_client = session.client('s3')
        self.iam_client = session.client('iam')
        
        # Get AWS account ID from the session
        self.account_id = self._get_account_id(session)
        
        # Use the model ID from your bedrock module
        self.llm_model_id = "anthropic.claude-3-5-sonnet-20241022-v2:0"
        
        logger.info("BedrockBatch initialized successfully")
    
    def _get_account_id(self, session) -> str:
        """Get AWS account ID from STS."""
        sts_client = session.client('sts')
        return sts_client.get_caller_identity()["Account"]
    
    def create_s3_bucket_if_not_exists(self, bucket_name: str) -> bool:
        """Create S3 bucket if it doesn't exist."""
        logger.info(f"Checking if S3 bucket {bucket_name} exists")
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                try:
                    logger.info(f"Bucket {bucket_name} does not exist. Creating new bucket")
                    if self.region != 'us-west-2':
                        self.s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': self.region
                            }
                        )
                    else:
                        self.s3_client.create_bucket(Bucket=bucket_name)
                    
                    logger.info(f"Created new bucket: {bucket_name}")
                    
                    waiter = self.s3_client.get_waiter('bucket_exists')
                    waiter.wait(Bucket=bucket_name)
                    logger.info(f"Bucket {bucket_name} is now available")
                    
                    return True
                except ClientError as create_error:
                    logger.error(f"Error creating bucket: {create_error}")
                    return False
            else:
                logger.error(f"Error checking bucket: {e}")
                return False

    def verify_s3_permissions(self, bucket_name: str) -> bool:
        """Verify S3 permissions for batch processing."""
        logger.info(f"Verifying S3 permissions for bucket: {bucket_name}")
        try:
            if not self.create_s3_bucket_if_not_exists(bucket_name):
                raise Exception(f"Failed to create or verify bucket {bucket_name}")
                
            logger.info("Testing S3 permissions...")
            self.s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key='test-permissions.txt',
                Body='Testing write permissions'
            )
            self.s3_client.delete_object(
                Bucket=bucket_name,
                Key='test-permissions.txt'
            )
            logger.info("Successfully verified S3 permissions")
            return True
        except ClientError as e:
            logger.error(f"S3 permission verification failed: {e}")
            return False

    def create_iam_role(self, role_name: str, bucket_name: str) -> str:
        """Create IAM role for Bedrock batch inference."""
        logger.info(f"Creating IAM role: {role_name}")
        trust_policy = {
            "Version": "2012-10-17",
            "Statement":[{
                "Effect": "Allow",
                "Principal": {
                    "Service": "bedrock.amazonaws.com"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": self.account_id
                    },
                    "ArnEquals": {
                        "aws:SourceArn": f"arn:aws:bedrock:{self.region}:{self.account_id}:model-invocation-job/*"
                    }
                }
            }]
        }

        permission_policy = {
            "Version": "2012-10-17",
            "Statement":[{
                "Effect": "Allow",
                "Action": [
                    "s3:*"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            }]
        }

        try:
            # Check if role already exists
            try:
                role_response = self.iam_client.get_role(RoleName=role_name)
                role_arn = role_response['Role']['Arn']
                logger.info(f"IAM role {role_name} already exists with ARN: {role_arn}")
                
                # Update trust policy
                self.iam_client.update_assume_role_policy(
                    RoleName=role_name,
                    PolicyDocument=json.dumps(trust_policy)
                )
                logger.info(f"Updated trust policy for existing role: {role_name}")
                
                # Update permission policy
                try:
                    self.iam_client.get_role_policy(
                        RoleName=role_name,
                        PolicyName=f"{role_name}-policy"
                    )
                    
                    # Update existing policy
                    self.iam_client.put_role_policy(
                        RoleName=role_name,
                        PolicyName=f"{role_name}-policy",
                        PolicyDocument=json.dumps(permission_policy)
                    )
                    logger.info(f"Updated permission policy for role: {role_name}")
                except ClientError:
                    # Create new policy if it doesn't exist
                    self.iam_client.put_role_policy(
                        RoleName=role_name,
                        PolicyName=f"{role_name}-policy",
                        PolicyDocument=json.dumps(permission_policy)
                    )
                    logger.info(f"Created new permission policy for role: {role_name}")
                    
            except ClientError:
                # Create new role if it doesn't exist
                role_response = self.iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy)
                )
                role_arn = role_response['Role']['Arn']
                logger.info(f"Created new IAM role: {role_name} with ARN: {role_arn}")

                # Attach permission policy
                self.iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName=f"{role_name}-policy",
                    PolicyDocument=json.dumps(permission_policy)
                )
                logger.info("Attached permission policy to new role")

            # Wait for role propagation
            logger.info("Waiting for IAM role to propagate...")
            time.sleep(50)
            return role_arn
        except ClientError as e:
            logger.error(f"Error creating/updating IAM role: {e}")
            raise

    def upload_file_to_s3(self, local_file_path: str, bucket_name: str, s3_key: str) -> None:
        """Upload file to S3 bucket."""
        logger.info(f"Uploading file {local_file_path} to S3 bucket {bucket_name} with key {s3_key}")
        try:
            if not Path(local_file_path).exists():
                raise FileNotFoundError(f"Input file not found: {local_file_path}")
                
            if not self.verify_s3_permissions(bucket_name):
                raise PermissionError("Failed to verify S3 permissions")

            self.s3_client.upload_file(local_file_path, bucket_name, s3_key)
            logger.info(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise

    def create_batch_inference_job(self, job_name: str, input_location: str, 
                                  output_location: str, role_arn: str) -> str:
        """Create a batch inference job."""
        logger.info(f"Creating batch inference job: {job_name}")
        try:
            response = self.bedrock_client.create_model_invocation_job(
                modelId=self.llm_model_id,
                jobName=job_name,
                inputDataConfig={
                    "s3InputDataConfig": {
                        "s3Uri": input_location
                    }
                },
                outputDataConfig={
                    "s3OutputDataConfig": {
                        "s3Uri": output_location
                    }
                },
                roleArn=role_arn
            )
            job_arn = response.get('jobArn')
            job_id = job_arn.split('/')[-1]
            logger.info(f"Created batch inference job: {job_id}")
            return job_id
        except ClientError as e:
            logger.error(f"Error creating batch inference job: {e}")
            raise

    def monitor_job_status(self, job_id: str) -> str:
        """Monitor the status of a batch job until completion."""
        logger.info(f"Monitoring job status for job ID: {job_id}")
        while True:
            try:
                job_arn = f"arn:aws:bedrock:{self.region}:{self.account_id}:model-invocation-job/{job_id}"
                response = self.bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)
                status = response['status']
                logger.info(f"Job status: {status}")

                if status.upper() == 'FAILED':
                    # Get and log the failure reason
                    failure_reason = response.get('message', 'No failure reason provided')
                    logger.error(f"Job failed with reason: {failure_reason}")
                    return status
                elif status.upper() in ['COMPLETED', 'STOPPED']:
                    return status

                time.sleep(30)  # Check every 30 seconds
            except ClientError as e:
                logger.error(f"Error monitoring job status: {e}")
                raise

    def download_batch_results(self, bucket_name: str, s3_prefix: str = "output/") -> Optional[str]:
        """Download batch inference results from S3."""
        logger.info(f"Downloading batch results from bucket: {bucket_name} with prefix: {s3_prefix}")
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=s3_prefix
            )

            jsonl_out_file = None
            latest_time = 0

            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.jsonl.out'):
                    logger.info(f"Found candidate output file: {key}")
                    if obj['LastModified'].timestamp() > latest_time:
                        latest_time = obj['LastModified'].timestamp()
                        jsonl_out_file = key

            if not jsonl_out_file:
                logger.error(f"No .jsonl.out file found in bucket {bucket_name} with prefix {s3_prefix}")
                return None

            output_file = f"downloaded_results_{int(time.time())}.jsonl.out"
            self.s3_client.download_file(
                bucket_name,
                jsonl_out_file,
                output_file
            )

            if os.path.exists(output_file):
                logger.info(f"Successfully downloaded file to: {output_file}")
            else:
                logger.error(f"Download reported success but file not found locally: {output_file}")

            return output_file

        except ClientError as e:
            logger.error(f"Error downloading batch results: {e}")
            return None

    def process_texts_individually(self, texts: List[str]) -> Dict[int, Dict[str, Any]]:
        """Process texts using direct API calls instead of batch processing."""
        logger.info(f"Processing {len(texts)} texts using direct API calls")
        results = {}
        
        for i, text in enumerate(texts):
            try:
                # Prepare prompt
                prompt = self.prepare_batch_prompt(text)
                
                # Prepare request body
                request_body = json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ]
                })
                
                # Call the LLM directly using your existing function
                logger.info(f"Processing text {i+1}/{len(texts)} using direct API call")
                response = invoke_llm(request_body)
                
                # Parse response
                response_body = json.loads(response['body'].read().decode('utf-8'))
                if 'content' in response_body and len(response_body['content']) > 0:
                    response_text = response_body['content'][0]['text']
                    
                    # Parse JSON from response text
                    try:
                        response_json = json.loads(response_text)
                        results[i] = response_json
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON from response for text {i}")
                        results[i] = {}
                else:
                    logger.warning(f"Empty or invalid response for text {i}")
                    results[i] = {}
                    
            except Exception as e:
                logger.error(f"Error processing text {i}: {str(e)}")
                results[i] = {}
        
        return results
    
    def generate_jsonl_from_raw_json_files(self, input_bucket: str, input_prefix: str, output_prefix: str,
                                           institution: str, config_path: str = "config.json",
                                           local_output_dir: str = "batch_inputs") -> List[str]:
        os.makedirs(local_output_dir, exist_ok=True)
        with open(config_path, "r", encoding="utf-8") as file:
            config = json.load(file)

        institution_data = config["templates"].get(institution, {})
        template = institution_data.get("template", {})
        valid_values = institution_data.get("valid_values", {})
        rules = institution_data.get("processing_rules", {}).get("rules", [])

        if not template:
            raise ValueError(f"No template found for institution '{institution}'")

        response = self.s3_client.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
        input_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

        jsonl_keys = []

        for file_key in input_files:
            file_obj = self.s3_client.get_object(Bucket=input_bucket, Key=file_key)
            input_json = json.loads(file_obj["Body"].read().decode("utf-8"))

            batch_inputs = []
            for idx, patient in enumerate(input_json, start=1):
                report = patient.get("report") or patient.get("Report", "").strip()
                results = patient.get("results") or patient.get("Results", [])

                if not report and not results:
                    continue

                record_id = f"PAT{idx:08d}"
                prompt = (
                    "You are an expert **pediatric** audiologist assistant responsible for extracting explicit hearing test data and classifying hearing loss with precision."
                    " Your classification must strictly follow given templates and clinical guidelines.\n\n"
                    "**Hearing Report:**\n\n"
                    f"{report}\n\n"
                    "**Audiometric Test Results:**\n\n"
                    f"{json.dumps(results, indent=4)}\n\n"
                    "**Classification Template:**\n\n"
                    f"{json.dumps(template, indent=4)}\n\n"
                    "**Valid Values:**\n"
                    f"```json\n{json.dumps(valid_values, indent=4)}\n```\n\n"
                    "**Classification Guidelines (MUST FOLLOW):**\n"
                    f"```json\n{json.dumps(rules, indent=4)}\n```\n\n"
                    "**Processing Rules (MUST Follow):**\n"
                    "- **Use only explicitly provided threshold values**; do not infer missing values.\n"
                    "- **If multiple severities are listed, assign the most severe classification.**\n\n"
                    "**Output Requirements:**\n"
                    "- Fill in missing values using classification rules.\n"
                    "- Assign the correct 'Better Ear' based on hearing loss severity.\n"
                    "- Use only valid options listed above (strict validation).\n"
                    "- Provide **reasoning** for each classification decision.\n"
                    "- Cite **guidelines** used in decisions.\n"
                    "- Return classification in **EXACT JSON format** as per the template, with no modifications.\n"
                    "- Provide **precise reasoning** for each classification.\n"
                    "- Make sure there is a **detailed, thorough, chain of thought reasoning for each attribute's output** and how it came to that conclusion."
                    "- Reasoning must include thorough reasoning for the left ear, right ear, and risk factors"
                    "- **Cite guideline numbers** when making classification decisions.\n"
                    "- **DO NOT include any additional explanations, assumptions, or commentary.**\n"
                )

                batch_inputs.append({
                    "recordId": record_id,
                    "modelInput": {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 4096, # 1024
                        "messages": [{
                            "role": "user",
                            "content": [{"type": "text", "text": prompt}]
                        }]
                    }
                })

            if not batch_inputs:
                continue

            input_filename = file_key.split("/")[-1].replace(".json", f"_{institution.lower()}_batch.jsonl")
            local_jsonl_path = os.path.join(local_output_dir, input_filename)

            with open(local_jsonl_path, "w", encoding="utf-8") as jsonl_file:
                for entry in batch_inputs:
                    jsonl_file.write(json.dumps(entry) + "\n")

            print({local_jsonl_path})
            output_s3_key = f"input/{input_filename}"
            self.s3_client.upload_file(local_jsonl_path, input_bucket, output_s3_key, ExtraArgs={"ContentType": "application/json"})
            jsonl_keys.append(output_s3_key)

        return jsonl_keys
    
    def process_batch_inference(self, input_bucket: str, input_prefix: str, output_prefix: str,
                                       institution: str, config_path: str = "config.json",
                                       local_output_dir: str = "batch_inputs") -> Dict[str, Any]:
        jsonl_keys = self.generate_jsonl_from_raw_json_files(
            input_bucket=input_bucket,
            input_prefix=input_prefix,
            output_prefix="input/",  # Save JSONL to input/ folder
            institution=institution,
            config_path=config_path,
            local_output_dir=local_output_dir
        )

        for key in jsonl_keys:
            local_path = os.path.join(local_output_dir, key.split("/")[-1])
            self.s3_client.download_file(input_bucket, key, local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                lines = [json.loads(l) for l in f.readlines()]
                real_records = [l for l in lines if not l["recordId"].startswith("dummy_")]

            texts = []
            record_map = {}
            for i, record in enumerate(real_records):
                messages = record.get("modelInput", {}).get("messages", [])
                if messages:
                    text = messages[0].get("content", [])[0].get("text", "")
                    texts.append(text)
                    record_map[record["recordId"]] = i

            if len(texts) >= 100:
                role_arn = self.create_iam_role(f"pediatric-aud-batch{int(time.time())}", input_bucket)
                input_uri = f"s3://{input_bucket}/{key}"
                output_uri = f"s3://{input_bucket}/{output_prefix}"
                job_id = self.create_batch_inference_job(
                    job_name=f"pediatric-aud-batch-{int(time.time())}",
                    input_location=input_uri,
                    output_location=output_uri,
                    role_arn=role_arn
                )
                status = self.monitor_job_status(job_id)
                if status == "COMPLETED" or "Completed":
                    # result_file = self.download_batch_results(input_bucket, s3_prefix=output_prefix)
                    # print({result_file})
                    # if result_file:
                    #     csv_path = self.jsonl_to_csv(result_file, institution=institution, config_path=config_path)
                    #     logger.info(f"CSV generated: {csv_path}")
                    
                    result_file = self.download_batch_results(input_bucket, s3_prefix=output_prefix)
                    print({result_file})
                    
                    # Determine original input file name to match with .json
                    original_name = key.split("/")[-1].replace("_batch.jsonl", ".json")

                    # Use original file name in CSV output
                    if result_file:
                        custom_csv_name = result_file.replace(".jsonl.out", f"_{original_name.replace('.json', '')}_output.csv")
                        csv_path = self.jsonl_to_csv(result_file, institution=institution, config_path=config_path)
                        os.rename(csv_path, custom_csv_name)
                        logger.info(f"CSV generated for {original_name}: {custom_csv_name}")

                    else:
                        results = {}
                else:
                    results = {}
            else:
                results = self.process_texts_individually(texts)

            # all_file_results[key] = results

        # return all_file_results
        
    def extract_and_clean_json(self, text: str) -> dict:
        """
        Attempts to robustly extract and clean a JSON object from a messy LLM string.
        Handles wrapping, escape issues, and incomplete formatting.
        """
        try:
            # Step 1: Try extracting from markdown block
            match = re.search(r"```json\s*({.*?})\s*```", text, re.DOTALL)
            if not match:
                match = re.search(r"({.*})", text, re.DOTALL)

            if not match:
                raise ValueError("No JSON block found in model output.")

            raw_json = match.group(1)

            # Step 2: Clean up badly escaped characters
            raw_json = raw_json.replace('\\"', '"')
            raw_json = raw_json.replace('\\\\n', '\\n')
            raw_json = raw_json.replace('\\n', '\n').replace('\\t', '\t')

            # Remove non-printable characters
            raw_json = re.sub(r'[\x00-\x1F\x7F]', '', raw_json)

            # Step 3: Try loading with standard JSON
            try:
                return json.loads(raw_json)
            except json.JSONDecodeError as e:
                # Step 4: Attempt partial fix by trimming to last closing brace
                if raw_json.count("{") > raw_json.count("}"):
                    raw_json += "}" * (raw_json.count("{") - raw_json.count("}"))

                try:
                    return json.loads(raw_json)
                except json.JSONDecodeError:
                    # Step 5: Final fallback: ast.literal_eval (can handle single quotes, minor syntax issues)
                    return ast.literal_eval(raw_json)

        except Exception as e:
            raise ValueError(f"JSON decode failed: {e}")

    def jsonl_to_csv(self, jsonl_filename: str, institution: str, config_path: str = "config.json") -> str:
        """
        Convert a .jsonl.out file to a CSV file based on institution-specific headers and mappings.

        Args:
            jsonl_filename: Path to the .jsonl.out file.
            institution: Institution name used to load config.
            config_path: Path to the config JSON file.

        Returns:
            Path to the generated CSV file.
        """
        config = self._load_config(config_path, institution)
        headers = config["csv_headers"]
        csv_filename = jsonl_filename.replace(".jsonl.out", f"_{institution.lower()}_output.csv")
        error_log = jsonl_filename.replace(".jsonl.out", f"_{institution.lower()}_error_log.txt")

        rows = []
        with open(jsonl_filename, "r", encoding="utf-8") as f_in, open(error_log, "w", encoding="utf-8") as f_log:
            for line_num, line in enumerate(f_in, 1):
                try:
                    record = json.loads(self._sanitize_line(line))
                    row = self._build_csv_row(record, headers, config, line_num)
                    if row:
                        rows.append(row)
                except Exception as e:
                    self._log_parsing_error(f_log, line_num, line, e)

        self._write_csv(csv_filename, headers, rows)
        logger.info(f"CSV written to: {csv_filename}")
        return csv_filename
    
    def _load_config(self, path: str, institution: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        if institution not in config["templates"]:
            raise ValueError(f"Institution '{institution}' not found in config")
        return config["templates"][institution]

    def _sanitize_line(self, line: str) -> str:
        clean = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', line)
        clean = re.sub(r'\\(?![btnfr"\\/])', r'\\\\', clean)
        return clean

    def _log_parsing_error(self, log_file, line_num: int, raw_line: str, error: Exception):
        log_file.write(f"[Line {line_num}] Error: {error}\n")
        log_file.write(f"Raw content: {raw_line}\n\n")

    def _build_csv_row(self, record: dict, headers: List[str], config: dict, line_number: int) -> Optional[List[str]]:
        patient_id = record.get("recordId", f"PAT{str(line_number).zfill(8)}")
        raw_report, test_results = self._extract_sections(record)

        try:
            raw_output = record.get("modelOutput", {}).get("content", [{}])[0].get("text", "")
            clean_output = self._sanitize_line(raw_output)
            attributes_json = self.extract_and_clean_json(clean_output)
        except Exception as e:
            logger.warning(f"Failed to extract JSON from record {patient_id}: {e}")
            return None

        attributes = attributes_json.get("Attributes", attributes_json)

        row = [patient_id, raw_report, test_results]
        for header in headers[3:]:  # Skip first 3 (ID, report, results)
            value = self._extract_value_by_header(attributes, header)
            row.append(value)
        return row

    def _extract_sections(self, record: dict) -> tuple[str, str]:
        raw_report = "NULL"
        audiometric_results = "NULL"
        messages = record.get("modelInput", {}).get("messages", [])
        for msg in messages:
            for content in msg.get("content", []):
                if content.get("type") == "text":
                    text = content.get("text", "")
                    if "**Hearing Report:**" in text:
                        raw_report = self._extract_between(text, "**Hearing Report:**", "**Audiometric Test Results:**")
                    if "**Audiometric Test Results:**" in text:
                        audiometric_results = self._extract_between(text, "**Audiometric Test Results:**", "**Classification Template:**")
        return raw_report, audiometric_results

    def _extract_between(self, text: str, start_marker: str, end_marker: str) -> str:
        try:
            return text.split(start_marker)[1].split(end_marker)[0].strip()
        except IndexError:
            return "NULL"

    def _extract_value_by_header(self, attributes: dict, header: str) -> str:
        path = self._map_header_to_path(header)
        parts = path.split(">")
        val = attributes
        for part in parts:
            val = val.get(part, "") if isinstance(val, dict) else ""
        if isinstance(val, list):
            return ", ".join(map(str, val))
        return val if isinstance(val, str) else json.dumps(val)

    def _map_header_to_path(self, header: str) -> str:
        if "Left Ear" in header:
            return f"Hearing Type>Left Ear>{header.split('Left Ear ')[1]}"
        elif "Right Ear" in header:
            return f"Hearing Type>Right Ear>{header.split('Right Ear ')[1]}"
        elif "Tier One" in header:
            return "Known Hearing Loss Risk Indicators>Risk Factors>Tier One"
        elif "Tier Two" in header:
            return "Known Hearing Loss Risk Indicators>Risk Factors>Tier Two"
        elif header == "Known Hearing Loss Risk":
            return "Known Hearing Loss Risk Indicators>Known Hearing Loss Risk"
        elif header == "Reasoning":
            return "Reasoning"
        return header

    def _write_csv(self, path: str, headers: List[str], rows: List[List[str]]):
        rows_sorted = sorted(rows, key=lambda r: int(re.search(r"PAT(\d+)", r[0]).group(1)) if re.search(r"PAT(\d+)", r[0]) else float('inf'))
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows_sorted:
                writer.writerow(row)

def main():
    input_bucket = "pallavi-bedrock-batch-inference"
    input_prefix = "meei-deidentidfied-data-raw/"
    output_prefix = "output/"
    institution = "Redcap"
    config_path = "config.json"
    local_output_dir = "batch_inputs"

    processor = BedrockBatch(region="us-west-2")

    results = processor.process_batch_inference(
        input_bucket=input_bucket,
        input_prefix=input_prefix,
        output_prefix=output_prefix,
        institution=institution,
        config_path=config_path,
        local_output_dir=local_output_dir
    )

    print("\n=== BATCH INFERENCE COMPLETED - CSV CREATED ===")
    
    # csv_path = processor.jsonl_to_csv(jsonl_filename="downloaded_results_1744047124.jsonl.out", institution=institution, config_path=config_path)
    # logger.info(f"CSV generated: {csv_path}")
    
if __name__ == "__main__":
    main()