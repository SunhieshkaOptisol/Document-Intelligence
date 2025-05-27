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
from multiprocessing import Pool, cpu_count
import traceback

def process_single_file_worker(args):
    """
    Worker function for processing a single file - must be at module level for pickling
    """
    file_path, credentials = args
    logger = setup_logger()
    
    try:
        logger.info(f"Worker processing file: {os.path.basename(file_path)}")
        
        # Extract content using Azure Document Intelligence
        try:
            logger.info(f"Extracting content from {file_path}")
            os.environ["VISION_KEY"] = credentials['azure_key']
            os.environ["VISION_ENDPOINT"] = credentials['azure_endpoint']
            azure_extractor = AzureDocumentIntelligence(file_path=file_path)
            text_content = azure_extractor.extract_text()
            extracted_content = {
                'text': text_content,
                'tables': [],
                'file_name': os.path.basename(file_path)
            }
        except Exception as e:
            logger.error(f"Error extracting content from {file_path}: {str(e)}")
            raise
        
        # Process with Pezzo AI to get structured data
        try:
            logger.info(f"Processing content with Pezzo for {extracted_content['file_name']}")
            
            # Initialize Pezzo renderer for this worker
            pezzo_renderer = PezzoPromptRenderer(
                api_key=credentials['pezzo_api_key'],
                project_id=credentials['PEZZO_PROJECT_ID_3'],
                environment=os.getenv('PEZZO_ENVIRONMENT', 'Production'),
                server_url=os.getenv('PEZZO_SERVER_URL')
            )
            
            # Format the content
            document_content = f"""
            Text from the document:
            {extracted_content['text']}

            Tables from the document:
            {json.dumps(extracted_content['tables'], indent=2)}
            """

            # Get and render the prompt
            prompt_template = pezzo_renderer.get_prompt("ExtractionPrompt")
            rendered_prompt = prompt_template.format(document_content=document_content)

            # Call the LLM
            openai_connector = AzureOpenAIConnector()
            model_deployment = os.getenv('AZURE_MODEL_DEPLOYMENT_NAME')
            if not model_deployment:
                raise ValueError("AZURE_MODEL_DEPLOYMENT_NAME environment variable is not set")
            llm = openai_connector.connect_azure_open_ai(model_deployment)
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
                'customer_address': str(extracted_data.get('customer_address', 'N/A'))
            }

            items = extracted_data.get('items', [])
            processed_records = []

            if not items:
                record = invoice_info.copy()
                record.update({
                    'item_description': 'N/A',
                    'qty': 'N/A',

                })
                processed_records.append(record)
            else:
                for item in items:
                    if isinstance(item, dict):
                        record = invoice_info.copy()
                        record.update({
                            'item_description': str(item.get('item_description', 'N/A')),
                            'qty': str(item.get('qty', 'N/A')),

                        })
                        processed_records.append(record)
            
            logger.info(f"Successfully processed {os.path.basename(file_path)} - {len(processed_records)} records")
            return processed_records
            
        except Exception as e:
            logger.error(f"Error processing with Pezzo for {file_path}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
            
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # Return error record to ensure file is not missed
        fieldnames = [
            'source_file', 'invoice_number', 'invoice_date', 'customer_name',
            'customer_address', 'item_description', 'qty', 'processing_error'
        ]
        
        error_record = {
            'source_file': os.path.basename(file_path),
            'processing_error': str(e),
            **{f: 'N/A' for f in fieldnames if f not in ['source_file', 'processing_error']}
        }
        return [error_record]


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
        # Store credentials for worker processes
        self._credentials = {
            'azure_endpoint': azure_endpoint,
            'azure_key': azure_key,
            'pezzo_api_key': pezzo_api_key,
            'PEZZO_PROJECT_ID_3': pezzo_project_id
        }

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
                'customer_address': str(extracted_data.get('customer_address', 'N/A'))
            }

            items = extracted_data.get('items', [])
            processed_records = []

            if not items:
                record = invoice_info.copy()
                record.update({
                    'item_description': 'N/A',
                    'qty': 'N/A',
                })
                processed_records.append(record)
            else:
                for item in items:
                    if isinstance(item, dict):
                        record = invoice_info.copy()
                        record.update({
                            'item_description': str(item.get('item_description', 'N/A')),
                            'qty': str(item.get('qty', 'N/A')),
                        })
                        processed_records.append(record)
            return processed_records
        except Exception as e:
            self.logger.error(f"Error processing with Pezzo: {str(e)}")
            raise

    def process_zip_file(self, zip_path: str) -> str:
        try:
            self.logger.info(f"Starting processing of zip file: {zip_path}")
            
            # Extract files from zip
            extracted_files = self.extract_zip_files(zip_path)
            if not extracted_files:
                raise Exception("No valid document files found in the zip archive")

            self.logger.info(f"Found {len(extracted_files)} files to process")

            # Get fieldnames for CSV
            fieldnames = self.get_csv_fieldnames()
            
            # Create output buffer
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            # Determine number of processes (use 75% of available CPUs, but at least 1)
            num_processes = max(1, int(cpu_count() * 0.75))
            self.logger.info(f"Using {num_processes} processes for parallel processing")

            # Prepare arguments for worker processes
            worker_args = [(file_path, self._credentials) for file_path in extracted_files]

            # Process files in parallel
            try:
                with Pool(processes=num_processes) as pool:
                    self.logger.info(f"Starting parallel processing of {len(worker_args)} files")
                    
                    # Use map to process all files
                    results = pool.map(process_single_file_worker, worker_args)
                    
                    self.logger.info(f"Parallel processing completed. Got {len(results)} results")
                    
                    # Process results and write to CSV
                    total_records = 0
                    files_processed = 0
                    files_with_errors = 0
                    
                    for i, file_results in enumerate(results):
                        if file_results:  # Ensure we have results
                            files_processed += 1
                            
                            # Check if this file had processing errors
                            has_error = any('processing_error' in record and record['processing_error'] != 'N/A' 
                                           for record in file_results if isinstance(record, dict))
                            if has_error:
                                files_with_errors += 1
                            
                            # Write all records for this file
                            for record in file_results:
                                if isinstance(record, dict):
                                    row_data = {field: record.get(field, 'N/A') for field in fieldnames}
                                    writer.writerow(row_data)
                                    total_records += 1
                        else:
                            # Handle case where worker returned None or empty list
                            self.logger.warning(f"No results returned for file index {i}")
                            files_with_errors += 1
                            # Write error record
                            error_record = {
                                'source_file': f'unknown_file_{i}',
                                'processing_error': 'No results returned from worker',
                                **{field: 'N/A' for field in fieldnames if field not in ['source_file', 'processing_error']}
                            }
                            writer.writerow(error_record)
                            total_records += 1

                    self.logger.info(f"Processing summary:")
                    self.logger.info(f"  - Total files found: {len(extracted_files)}")
                    self.logger.info(f"  - Files processed: {files_processed}")
                    self.logger.info(f"  - Files with errors: {files_with_errors}")
                    self.logger.info(f"  - Total records generated: {total_records}")

            except Exception as e:
                self.logger.error(f"Error in parallel processing: {str(e)}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                raise

            csv_content = output.getvalue()
            self.logger.info(f"Generated CSV with {len(csv_content.split(chr(10))) - 1} total rows")
            
            return csv_content
            
        except Exception as e:
            self.logger.error(f"Error processing zip file: {str(e)}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def get_csv_fieldnames(self) -> List[str]:
        return [
            'source_file',
            'invoice_number',
            'invoice_date',
            'customer_name',
            'customer_address',
            'item_description',
            'qty',
            'processing_error'
        ]

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