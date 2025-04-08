# Pediatric Audiology Classification

# Collaboration

Thanks for your interest in our solution. Having specific examples of replication and cloning allows us to continue to grow and scale our work. If you clone or download this repository, kindly shoot us a quick email to let us know you are interested in this work!

[wwps-cic@amazon.com]

# Disclaimers

**Customers are responsible for making their own independent assessment of the information in this document.**

**This document:**

(a) is for informational purposes only,

(b) represents current AWS product offerings and practices, which are subject to change without notice, and

(c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS products or services are provided "as is" without warranties, representations, or conditions of any kind, whether express or implied. The responsibilities and liabilities of AWS to its customers are controlled by AWS agreements, and this document is not part of, nor does it modify, any agreement between AWS and its customers.

(d) is not to be considered a recommendation or viewpoint of AWS

**Additionally, all prototype code and associated assets should be considered:**

(a) as-is and without warranties

(b) not suitable for production environments

(d) to include shortcuts in order to support rapid prototyping such as, but not limitted to, relaxed authentication and authorization and a lack of strict adherence to security best practices

**All work produced is open source. More information can be found in the GitHub repo.**

## Authors

- Pallavi Das - padas@calpoly.edu

## Table of Contents

- [Collaboration](#collaboration)
- [Disclaimers](#disclaimers)
- [Authors](#authors)
- [Overview](#overview)
- [High Level Description of Workflow](#high-level-description-of-workflow)
  - [Step 1: Generate Reference Embeddings](#step-1-generate-reference-embeddings)
  - [Step 2: Classify and Extract Information](#step-2-classify-and-extract-information)
  - [Final Output Details](#final-output-details)
- [Steps to Deploy and Configure the System](#steps-to-deploy-and-configure-the-system)
  - [Before We Get Started](#before-we-get-started)
  - [1. Deploy an EC2 Instance](#1-deploy-an-ec2-instance)
  - [2. Pull the Git Repository onto the EC2 Instance](#2-pull-the-git-repository-onto-the-ec2-instance)
  - [3. Create a Virtual Environment](#3-create-a-virtual-environment)
  - [4. Activate the Virtual Environment](#4-activate-the-virtual-environment)
  - [5. Install the Required Packages](#5-install-the-required-packages)
  - [6. Set Environment Variables](#6-set-environment-variables)
  - [7. Run the Embeddings Pipeline](#7-run-the-embeddings-pipeline)
  - [8. Classify and Extract Information from an eCR](#8-classify-and-extract-information-from-an-ecr)
- [Recommended Customer Workflow](#recommended-customer-workflow)
  - [Concept Classification Workflow](#concept-classification-workflow)
  - [Soft Attribute Inference Workflow](#soft-attribute-inference-workflow)
- [Customizing LLM Soft Attribute Prompt](#customizing-llm-soft-attribute-prompt)
- [Known Bugs/Concerns](#known-bugsconcerns)
- [Support](#support)

## Overview

- The [DxHub](https://dxhub.calpoly.edu/challenges/) developed a Python script leveraging AWS Bedrock to classify pediatric audiology reports and audiometric test results, as well as extract structured information for downstream analytics and clinical reporting. It automates batch inference processing of hearing test reports, applying strict clinical templates and rules to ensure consistent and explainable classifications.

## High Level Description of Workflow

[Feel free to skip to the deployment section](#steps-to-deploy-and-configure-the-system) if you just want to get started. This is just a look on what this process is doing and the "theory" behind it.

### Step 1: Generate Prompted Inputs

- Pull data from S3 and read raw JSON files (e.g., de-identified patient reports)
- Use institution templates and clinical rules in a logic engine (`config.json`) to generate structured prompts in jsonl format for the AWS Bedrock's Claude 3.5 Sonnet

### Step 2: Submit Batch Inference Job

-  Upload .jsonl batch input and start Bedrock batch job if greater than 100 input prompts
- Monitor status and download jsonl.out results once complete


### Step 3: Parse Output and Write CSV
- Extract structured classifications and write final CSVs with clinical reasoning based on the institution provided

### Final Output Details

The final output is saved as `downloaded_results_{int(time.time())}.jsonl.out` and contains the following:

- The patient index
- The raw report
- The audiometric test results
- Hearing Types for each ear
- Known Risk Factors (if applicable by institution: e.g. Redcap)
- Reasoning

#### Example Output Structure

| Field | Value |
|-------|-------|
| **Patient Index** | PAT00000001 |
| **Raw Report** | REASON FOR TEST: ... MEASUREMENT:... INTERPRETATION:... RECOMMENDATIONs:... |
| **Audiometric Test Results** | `[{"TransducerType": "BONE", "Side": "BINAURAL", "StimType": "TONE BURST", "Frequency": 2000, "DB_HL": 45, "Type": "THRESHOLD"},...]` |
| **Left Ear Type** | Sensorineural |
| **Left Ear Degree** | Mild (26-40 dB HL) |
| **Right Ear Type** | Unknown |
| **Right Ear Degree** | Severe (71-90 dB HL) |
| **Known Hearing Loss Risk** | Yes |
| **Tier One Risk Factors** | cCMV |
| **Tier Two Risk Factors** | *(empty)* |
| **Reasoning** | Left Ear: Air conduction thresholds show 40 dB HL at 1k–4k Hz and 45 dB at 8k Hz. Report explicitly states "mild sensorineural hearing loss". Type classified as Sensorineural per guidelines #6. Degree classified as Mild (26–40 dB HL) based on majority of thresholds (Guidelines #9a).  Right Ear: Air conduction thresholds show 70 dB HL at 1k–2k Hz and 85 dB HL at 4k Hz. Report states "severe to profound loss of unknown nature". Per guideline #12, classified as Unknown type with Severe degree based on threshold values falling within 71–90 dB HL range.  Risk Factors: Report explicitly mentions "positive CMV findings at birth", which aligns with "cCMV" in Tier One risk factors. Per guideline #1, presence of Tier One factor requires "Known Hearing Loss Risk" marked as "Yes". Guidelines #18 confirms only listing risk factors when Known Hearing Loss Risk is Yes. |

## Steps to Deploy and Configure the System

### Before We Get Started

- Request and ensure model access within AWS Bedrock, specifically:
  - Claude 3.5 Sonnet V2

The corresponding model ID is:

```
anthropic.claude-3-5-sonnet-20241022-v2:0
```

### System Configuration (Required)

The system is configurable by institution via `config.json`.
Each institution config defines:
- template: The desired JSON output structure
- valid_values: Allowed values per field
- processing_rules: Clinical classification logic
- csv_headers: Columns for final output

Example institutions included:
- MassEyeAndEar
- Redcap
- CDC
- Dawn

Ensure your input bucket contains raw json files.

### 1. Environment Step

```bash
git clone https://github.com/your-org/pediatric-aud-batch.git
cd pediatric-aud-batch
python3 -m venv .venv
source .venv/bin/activate
pip install boto3
```

### 2. Update Configuration (Template or Logic Engine) - Optional

- Edit `config.json` to reflect your institution’s template and logic.


### 3. Set Required AWS Permissions
Ensure the user/instance has access to:
- AmazonBedrockFullAccess
- AmazonS3FullAccess
- IAM:CreateRole / PutRolePolicy

### 5. Install the required packages:

```bash
pip install -r requirements.txt
```

### 6. Set environment variables:

- Create a .env file to store your environment variables

```bash
cp .env.example .env
```

- Add your AWS credentials to the .env file under the appropriate variable names

### 7. Run the Batch Pipeline:

- Run the main pipeline (this script will auto-create roles, verify S3, upload, monitor, and download):

```bash
python3 automated_aud_batch.py
```
This script runs the full pipeline for one institution (Redcap by default).


### Output Files
Each batch job produces:
- *_output.csv: Final structured results
- *_error_log.txt: Any records that failed JSON parsing
- *.jsonl.out: Raw Claude outputs downloaded from S3

CSV files contain:
- Left/Right Ear Type and Degree
- Risk Factor Flags (e.g., Tier 1, Tier 2) if applicable
- Explanation + Guideline citations

## Known Bugs/Concerns

- Model output sometimes needs JSON cleanup in order to process all outputs to CSV
- Output format assumes record-wise responses; unexpected format may fail silently
- Some models may not hold full comprehensive audiology knowledge
- Occasionally, a batch inference job will take more time than usual to complete, pausing on the Scheduled Status

## Support

For any queries or issues, please contact:

- Darren Kraker - Sr Solutions Architect - dkraker@amazon.com
- Pallavi Das - Software Developer Intern - padas@calpoly.edu
