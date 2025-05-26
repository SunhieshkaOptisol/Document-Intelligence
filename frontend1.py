import os
import tempfile
import streamlit as st
import pandas as pd
from io import StringIO
import zipfile
from datetime import datetime
from dotenv import load_dotenv
from backend import DocumentProcessor

# Load environment variables
load_dotenv()

# Import the backend processor for zip processing
try:
    from invoice_processor import create_processor, InvoiceProcessor
    ZIP_PROCESSOR_AVAILABLE = True
except ImportError:
    ZIP_PROCESSOR_AVAILABLE = False

st.set_page_config(page_title="Document Intelligence", page_icon="📄", layout="wide")

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #c3e6cb;
    }
    .error-box {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #f5c6cb;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state variables if they don't exist
if 'invoice_content' not in st.session_state:
    st.session_state.invoice_content = None
if 'po_content' not in st.session_state:
    st.session_state.po_content = None
if 'invoice_path' not in st.session_state:
    st.session_state.invoice_path = None
if 'po_path' not in st.session_state:
    st.session_state.po_path = None
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'csv_content' not in st.session_state:
    st.session_state.csv_content = None

# Initialize the document processor
@st.cache_resource
def get_document_processor():
    return DocumentProcessor()

processor = get_document_processor()

def process_uploaded_file(uploaded_file, document_type):
    st.write(f"Processing {document_type}: {uploaded_file.name}")
    progress = st.progress(0)
    status = st.empty()
    status.text("Saving file...")
    
    # Get file extension
    file_extension = os.path.splitext(uploaded_file.name)[1].lower()
    
    # Create temp file with appropriate extension
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
        temp_file.write(uploaded_file.getvalue())
        temp_path = temp_file.name
 
    try:
        progress.progress(20)
        status.text("Extracting data...")
        result = processor.process_file(temp_path, document_type)
   
        progress.progress(100)
        status.text("Done ✅")
 
        with st.expander(f"Results: {uploaded_file.name}", expanded=True):
            st.markdown("Extracted Successfully")
        
        # Store the content and path in session state
        if document_type == "invoice":
            st.session_state.invoice_content = result
            st.session_state.invoice_path = temp_path
        else:
            st.session_state.po_content = result
            st.session_state.po_path = temp_path
            
        return result, temp_path
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None, temp_path

def validate_zip_file(uploaded_file):
    """Validate the uploaded zip file"""
    if uploaded_file is None:
        return False, "No file uploaded"
    
    if not uploaded_file.name.lower().endswith('.zip'):
        return False, "Please upload a ZIP file"
    
    # Check if zip file contains valid documents
    try:
        with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            valid_extensions = ('.pdf', '.png', '.jpg', '.jpeg', '.tiff', '.bmp')
            valid_files = [f for f in file_list if f.lower().endswith(valid_extensions)]
            
            if not valid_files:
                return False, "ZIP file must contain at least one document file (PDF, PNG, JPG, JPEG, TIFF, BMP)"
            
            return True, f"Found {len(valid_files)} valid document(s) in the ZIP file"
    
    except zipfile.BadZipFile:
        return False, "Invalid ZIP file format"

def process_zip_invoices(uploaded_file):
    """Process the uploaded invoices from ZIP file"""
    try:
        # Create a progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("Initializing processor...")
        progress_bar.progress(10)
        
        # Create processor instance
        zip_processor = create_processor()
        
        status_text.text("Saving uploaded file...")
        progress_bar.progress(20)
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
        
        status_text.text("Processing invoices...")
        progress_bar.progress(40)
        
        # Process the zip file
        csv_content = zip_processor.process_zip_file(tmp_file_path)
        
        status_text.text("Finalizing results...")
        progress_bar.progress(90)
        
        # Clean up temporary file
        os.unlink(tmp_file_path)
        
        # Store results in session state
        st.session_state.csv_content = csv_content
        st.session_state.processing_complete = True
        
        progress_bar.progress(100)
        status_text.text("✅ Processing complete!")
        
        st.success("🎉 Invoices processed successfully!")
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error processing invoices: {str(e)}")
        if 'tmp_file_path' in locals():
            try:
                os.unlink(tmp_file_path)
            except:
                pass

def display_zip_results():
    """Display ZIP processing results"""
    st.header("📊 Processing Results")
    
    # Parse CSV content for preview
    try:
        df = pd.read_csv(StringIO(st.session_state.csv_content))
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader("📋 Data Preview")
            st.dataframe(df, use_container_width=True, height=400)
        
        with col2:
            st.subheader("📈 Summary")
            st.metric("Total Records", len(df))
            st.metric("Total Columns", len(df.columns))
            
            # File download
            csv_filename = f"processed_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            st.download_button(
                label="📥 Download CSV",
                data=st.session_state.csv_content,
                file_name=csv_filename,
                mime="text/csv",
                type="primary",
                use_container_width=True
            )
    
    except Exception as e:
        st.error(f"Error displaying results: {str(e)}")
        
        # Still provide download option for raw CSV
        csv_filename = f"processed_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        st.download_button(
            label="📥 Download Raw CSV",
            data=st.session_state.csv_content,
            file_name=csv_filename,
            mime="text/csv"
        )

def invoice_parser_app():
    """
    Invoice Parser Application
    """
    st.title("Invoice Parser")
    st.markdown("Upload invoice PDFs to extract structured data")
    
    # Check for environment variables
    if not processor.check_credentials():
        st.error("Azure Document Intelligence credentials not found. Please check your environment variables.")
        return
    
    # File uploader section
    st.subheader("Upload PDF Invoices")
    uploaded_files = st.file_uploader("Choose PDF files", type="pdf", accept_multiple_files=True, key="invoice_parser_uploader")
    
    if uploaded_files:
        st.write(f"Uploaded {len(uploaded_files)} file(s)")
        
        # Process files when user clicks the button
        if st.button("Process Files"):
            for uploaded_file in uploaded_files:
                # Create progress bar for this file
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text(f"Processing: {uploaded_file.name}...")
                
                # Save uploaded file to temporary location
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                    temp_file.write(uploaded_file.getvalue())
                    temp_file_path = temp_file.name
                
                try:
                    progress_bar.progress(25)
                    status_text.text(f"Analyzing document: {uploaded_file.name}")
                    
                    # Process the PDF
                    result = processor.process_invoice_pdf(temp_file_path).replace("```markdown","").replace("```","")
                    
                    progress_bar.progress(100)
                    status_text.text(f"Completed: {uploaded_file.name}")
                    
                    # Display results in an expander
                    with st.expander(f"Results for {uploaded_file.name}", expanded=True):
                        st.markdown(result)
                        
                        # Add download button for the results
                        st.download_button(
                            label="Download results as markdown",
                            data=result,
                            file_name=f"{os.path.splitext(uploaded_file.name)[0]}_results.md",
                            mime="text/markdown"
                        )               
                except Exception as e:
                    progress_bar.progress(100)
                    status_text.text(f"Error processing: {uploaded_file.name}")
                    st.error(f"Error processing {uploaded_file.name}: {str(e)}")
                
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                
            st.success("All files processed!")
    else:
        st.info("Please upload PDF invoice files to begin")

def invoice_po_comparison_app():
    """
    Invoice and PO Comparison Application
    """
    st.title("📄 Invoice and PO Comparison")
    st.markdown("Upload Invoices or Purchase Orders (PDF or CSV) and compare them.")
 
    # Check for environment variables
    if not processor.check_credentials():
        st.error("Azure Vision credentials missing.")
        return
 
    col1, col2 = st.columns(2)
 
    with col1:
        st.subheader("Invoice")
        invoice_file = st.file_uploader("Upload Invoice (PDF or CSV)", type=["pdf", "csv"], key="invoice_comparison")
        if invoice_file and st.button("Extract Invoice"):
            _, temp_path = process_uploaded_file(invoice_file, "invoice")
 
    with col2:
        st.subheader("Purchase Order")
        po_file = st.file_uploader("Upload PO (PDF or CSV)", type=["pdf", "csv"], key="po_comparison")
        if po_file and st.button("Extract Purchase Order"):
            _, temp_path = process_uploaded_file(po_file, "purchase_order")
 
    st.markdown("---")
    st.subheader("📊 Compare Invoice and Purchase Order")
 
    # Debug information
    if st.checkbox("Show debug info"):
        st.write("Invoice path:", st.session_state.invoice_path)
        st.write("PO path:", st.session_state.po_path)
        st.write("Invoice content exists:", st.session_state.invoice_content is not None)
        st.write("PO content exists:", st.session_state.po_content is not None)
 
    if st.session_state.invoice_path and st.session_state.po_path:
        if st.button("Compare Documents"):
            with st.spinner("Generating comparison summary..."):
                comparison = processor.generate_comparison_summary(
                    st.session_state.invoice_content, 
                    st.session_state.po_content
                )
                st.markdown(comparison)
                st.download_button(
                    "Download Comparison",
                    data=comparison,
                    file_name="invoice_po_comparison.md",
                    mime="text/markdown"
                )
    else:
        st.info("Upload both Invoice and Purchase Order to enable comparison.")

def document_parsing_app():
    """
    Document Parsing Application
    """
    st.title("Document Parser")
    st.markdown("Upload PDFs of invoices, timesheets, combined documents or multiple documents for detailed extraction")
    
    # Check for environment variables
    if not processor.check_credentials():
        st.error("Azure Document Intelligence credentials not found. Please check your environment variables.")
        return
    
    # Document type selection dropdown
    document_type = st.selectbox(
        "Select document type",
        options=["Invoice", "Timesheet", "Digital Invoice and Timesheet", "Multiple Timesheets"],
        help="Select the type of document you are uploading"
    )
    
    # File uploader
    uploaded_files = st.file_uploader("Upload PDF documents", 
                                     type=['pdf'], 
                                     accept_multiple_files=True,
                                     key="advanced_parser_uploader")
    
    if uploaded_files:
        if st.button("Process Files", key="process_advanced_files"):            
            with st.spinner("Processing files..."):
                # Process each file
                for uploaded_file in uploaded_files:
                    st.subheader(f"Processing: {uploaded_file.name}")
                    
                    # Process the file with the selected document type
                    result = processor.process_pdf_advanced(uploaded_file, document_type).replace("```markdown","").replace("```","")
                    
                    # Create a container for the rendered markdown
                    table_container = st.container()
                    with table_container:
                        # Render the markdown as a table
                        st.markdown(result, unsafe_allow_html=True)
                    
                    # Add download button
                    st.download_button(
                        label="Download results as markdown",
                        data=result,
                        file_name=f"{os.path.splitext(uploaded_file.name)[0]}_results.md",
                        mime="text/markdown"
                    )
                    
                    # Add a divider between files
                    st.markdown("---")
                
                st.success("All files processed successfully!")
    else:
        st.info("Please upload PDF files to begin")

def zip_invoice_processor_app():
    """
    ZIP Invoice Processing Application
    """
    st.markdown('<h1 class="main-header">📄 ZIP Invoice Processing System</h1>', unsafe_allow_html=True)
    st.markdown("Upload a ZIP file containing multiple invoices for batch processing")
    
    # Check if ZIP processor is available
    if not ZIP_PROCESSOR_AVAILABLE:
        st.error("❌ ZIP Invoice Processor is not available. Please ensure 'invoice_processor' module is installed.")
        return
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a ZIP file containing invoices",
        type=['zip'],
        help="Upload a ZIP file containing invoice documents in PDF, PNG, JPG, JPEG, TIFF, or BMP format",
        key="zip_invoice_uploader"
    )
    
    # Validate uploaded file
    if uploaded_file:
        is_valid, message = validate_zip_file(uploaded_file)
        if is_valid:
            st.markdown(f'<div class="success-box">✅ {message}</div>', unsafe_allow_html=True)
            
            # Process button
            if st.button("🚀 Process ZIP Invoices", type="primary", use_container_width=True):
                process_zip_invoices(uploaded_file)
        else:
            st.markdown(f'<div class="error-box">❌ {message}</div>', unsafe_allow_html=True)
    
    # Results section
    if st.session_state.processing_complete and st.session_state.csv_content:
        display_zip_results()

def main():
    """
    Main Streamlit application with app selector
    """
    # Create a sidebar with app selection
    st.sidebar.title("Document Intelligence")
    app_options = ["Invoice Parser", "Invoice-PO Comparison", "Document Parser (AU)", "ZIP Invoice Processor"]
    app_mode = st.sidebar.selectbox(
        "Choose Application Mode",
        app_options
    )
    
    # Display app information in sidebar
    st.sidebar.markdown("---")
    st.sidebar.info(
        """
        This application provides four tools:
        
        - **Invoice Parser**: Process individual and multilingual invoices to extract key information.
        - **Invoice-PO Comparison**: Compare invoices against purchase orders to identify discrepancies.
        - **Document Parser (AU)**: Extract structured information from invoices, timesheets, combined documents or multiple documents.
        - **ZIP Invoice Processor**: Batch process multiple invoices from a ZIP file and export results as CSV.
        """
    )
    
    # Run the selected app
    if app_mode == "Invoice Parser":
        invoice_parser_app()
    elif app_mode == "Invoice-PO Comparison":
        invoice_po_comparison_app()
    elif app_mode == "Document Parser (AU)":
        document_parsing_app()
    else:
        zip_invoice_processor_app()

if __name__ == "__main__":
    main()