import os
import json
import csv
import zipfile
import tempfile
from typing import List, Dict, Any
from io import StringIO
import pandas as pd
from elsai_core.model import AzureOpenAIConnector
from elsai_core.extractors.azure_document_intelligence import AzureDocumentIntelligence
from elsai_core.config.loggerConfig import setup_logger
from elsai_core.prompts import PezzoPromptRenderer

class InvoiceProcessor:
    def __init__(self, azure_endpoint: str, azure_key: str, pezzo_api_key: str, pezzo_project_id: str):
        self.logger = setup_logger()
        self.azure_endpoint = azure_endpoint
        self.azure_key = azure_key
        self.pezzo_renderer = PezzoPromptRenderer(
            api_key=pezzo_api_key,
            project_id=pezzo_project_id,
            environment=os.getenv('PEZZO_ENVIRONMENT', 'Production'),
            server_url=os.getenv('PEZZO_SERVER_URL')
        )
        self.openai_connector = AzureOpenAIConnector()

    def extract_zip_files(self, zip_path: str) -> List[str]:
        extracted_files = []
        temp_dir = tempfile.mkdtemp()
        try:
            self.logger.info(f"Extracting ZIP file: {zip_path}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if file.lower().endswith(('.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.bmp')):
                        extracted_files.append(file_path)
            self.logger.info(f"Extracted {len(extracted_files)} valid document files")
        except Exception as e:
            self.logger.error(f"Error extracting zip file: {str(e)}")
            raise
        return extracted_files

    def extract_document_content(self, file_path: str) -> Dict[str, Any]:
        try:
            self.logger.info(f"Extracting content from {file_path}")
            os.environ["VISION_KEY"] = self.azure_key
            os.environ["VISION_ENDPOINT"] = self.azure_endpoint
            azure_extractor = AzureDocumentIntelligence(file_path=file_path)
            text_content = azure_extractor.extract_text()
            return {
                'text': text_content,
                'tables': [],
                'file_name': os.path.basename(file_path)
            }
        except Exception as e:
            self.logger.error(f"Error extracting content from {file_path}: {str(e)}")
            raise

    def process_with_pezzo(self, extracted_content: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            self.logger.info(f"Processing content with Pezzo for {extracted_content['file_name']}")

            # Format the content
            document_content = f"""
            Text from the document:
            {extracted_content['text']}

            Tables from the document:
            {json.dumps(extracted_content['tables'], indent=2)}
            """

            # Get and render the prompt
            prompt_template = self.pezzo_renderer.get_prompt("ExtractionPrompt")
            rendered_prompt = prompt_template.format(document_content=document_content)

            # Call the LLM
            model_deployment = os.getenv('AZURE_MODEL_DEPLOYMENT_NAME')
            if not model_deployment:
                raise ValueError("AZURE_MODEL_DEPLOYMENT_NAME environment variable is not set")
            llm = self.openai_connector.connect_azure_open_ai(model_deployment)
            response = llm.invoke(rendered_prompt)

            response_text = response.content.strip()
            if response_text.startswith('```json'):
                response_text = response_text[len('```json'):].strip()
            if response_text.endswith('```'):
                response_text = response_text[:-len('```')].strip()

            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                extracted_data = json.loads(json_str)
            else:
                raise json.JSONDecodeError("No JSON object found in response", response_text, 0)

            invoice_info = {
                'source_file': extracted_content['file_name'],
                'invoice_number': str(extracted_data.get('invoice_number', 'N/A')),
                'invoice_date': str(extracted_data.get('invoice_date', 'N/A')),
                'customer_name': str(extracted_data.get('customer_name', 'N/A')),
                'customer_address': str(extracted_data.get('customer_address', 'N/A')),
                'customer_email': str(extracted_data.get('customer_email', 'N/A')),
                'customer_phone': str(extracted_data.get('customer_phone', 'N/A')),
                'vendor_name': str(extracted_data.get('vendor_name', 'N/A')),
                'vendor_address': str(extracted_data.get('vendor_address', 'N/A'))
            }

            items = extracted_data.get('items', [])
            processed_records = []

            if not items:
                record = invoice_info.copy()
                record.update({
                    'item_description': 'N/A',
                    'qty': 'N/A',
                    'unit_price': 'N/A',
                    'item_total_amount': 'N/A'
                })
                processed_records.append(record)
            else:
                for item in items:
                    if isinstance(item, dict):
                        record = invoice_info.copy()
                        record.update({
                            'item_description': str(item.get('item_description', 'N/A')),
                            'qty': str(item.get('qty', 'N/A')),
                            'unit_price': str(item.get('unit_price', 'N/A')),
                            'item_total_amount': str(item.get('total_amount', 'N/A'))
                        })
                        processed_records.append(record)
            return processed_records
        except Exception as e:
            self.logger.error(f"Error processing with Pezzo: {str(e)}")
            raise

    def process_zip_file(self, zip_path: str) -> str:
        try:
            self.logger.info(f"Starting processing of zip file: {zip_path}")
            extracted_files = self.extract_zip_files(zip_path)
            if not extracted_files:
                raise Exception("No valid document files found in the zip archive")

            fieldnames = [
                'source_file',
                'invoice_number',
                'invoice_date',
                'customer_name',
                'customer_address',
                'customer_email',
                'customer_phone',
                'vendor_name',
                'vendor_address',
                'item_description',
                'qty',
                'unit_price',
                'item_total_amount',
                'processing_error'
            ]

            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for file_path in extracted_files:
                file_name = os.path.basename(file_path)
                try:
                    extracted_content = self.extract_document_content(file_path)
                    structured_data_list = self.process_with_pezzo(extracted_content)
                    for record in structured_data_list:
                        row_data = {field: record.get(field, 'N/A') for field in fieldnames}
                        writer.writerow(row_data)
                except Exception as e:
                    writer.writerow({
                        'source_file': file_name,
                        'processing_error': str(e),
                        **{f: 'N/A' for f in fieldnames if f not in ['source_file', 'processing_error']}
                    })
            return output.getvalue()
        except Exception as e:
            self.logger.error(f"Error processing zip file: {str(e)}")
            raise

    def convert_to_csv(self, data: List[Dict[str, Any]]) -> str:
        if not data:
            self.logger.warning("No data to convert to CSV")
            return "No data to convert"
        all_keys = set()
        for record in data:
            if isinstance(record, dict):
                all_keys.update(record.keys())
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=sorted(all_keys))
        writer.writeheader()
        for record in data:
            clean_record = {k: (v if v is not None else "N/A") for k, v in record.items()}
            writer.writerow(clean_record)
        return output.getvalue()

def create_processor():
    AZURE_ENDPOINT = os.getenv('VISION_ENDPOINT')
    AZURE_KEY = os.getenv('VISION_KEY')
    PEZZO_API_KEY = os.getenv('PEZZO_API_KEY')
    PEZZO_PROJECT_ID = os.getenv('PEZZO_PROJECT_ID_3')
    if not all([AZURE_ENDPOINT, AZURE_KEY, PEZZO_API_KEY, PEZZO_PROJECT_ID]):
        raise ValueError("Missing required environment variables for Azure or Pezzo configuration")
    return InvoiceProcessor(
        azure_endpoint=AZURE_ENDPOINT,
        azure_key=AZURE_KEY,
        pezzo_api_key=PEZZO_API_KEY,
        pezzo_project_id=PEZZO_PROJECT_ID
    )
