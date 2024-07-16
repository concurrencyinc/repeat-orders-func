import instructor
from pydantic import BaseModel, Field
from openai import AzureOpenAI
from typing import List, Optional
import usaddress
from fuzzywuzzy import fuzz

import logging
import re

from globals import system_message_pdf_extraction

class ProductTableEntry(BaseModel):
    description: str = Field(default="")
    quantity: int = Field(default=0)
    price: float = Field(default=0.0)
    specifications: Optional[str] = Field(default=None)

class ProductTable(BaseModel):
    total_price: float = Field(default=0.0)
    entries: List[ProductTableEntry] = Field(default_factory=list)

class OrderInfo(BaseModel):
    company_name: str = Field(default="")
    order_number: str = Field(default="")
    shipping_address: Optional[str] = Field(default=None)
    billing_address: Optional[str] = Field(default=None)
    order_date: Optional[str]  = Field(default=None) # You might want to use a date type here

class ExtractedData(BaseModel):
    product_table: ProductTable = Field(default_factory=ProductTable)
    other_info: OrderInfo = Field(default_factory=OrderInfo)


class ParsedAddress(BaseModel):
    street_line_1: str = Field(default="")
    street_line_2: str = Field(default="")
    city: str = Field(default="")
    state: str = Field(default="")
    zipcode: str = Field(default="")
    name: str = Field(default="")

class ExtendedOrderInfo(OrderInfo):
    shipping_address_parsed: Optional[ParsedAddress] = None
    billing_address_parsed: Optional[ParsedAddress] = None

    def parse_addresses(self):
        self.shipping_address_parsed = self._parse_address(self.shipping_address)
        self.billing_address_parsed = self._parse_address(self.billing_address)

    @staticmethod
    def _parse_address(address: str) -> ParsedAddress:
        import usaddress

        try:
            preprocessed_address = preprocess_address(address)
            logging.info(f"Preprocessed Address: {preprocessed_address}") 
            address_dict, address_type = usaddress.tag(preprocessed_address)

            # Handle PO Box addresses
            if any([po_box in preprocessed_address for po_box in ['PO BOX', 'P.O. BOX', 'PO Box', 'POBox']]):
                street_line_1 = ' '.join([
                    address_dict.get('USPSBoxType', ''),
                    address_dict.get('USPSBoxID', '')
                ]).strip()
            else:
                street_line_1 = ' '.join([
                    address_dict.get('AddressNumber', ''),
                    address_dict.get('StreetNamePreDirectional', ''),
                    address_dict.get('StreetName', ''),
                    address_dict.get('StreetNamePostType', ''),
                    address_dict.get('StreetNamePostDirectional', '')
                ]).strip()
            
            street_line_2 = ' '.join([
                address_dict.get('OccupancyType', ''),
                address_dict.get('OccupancyIdentifier', '')
            ]).strip()

            return ParsedAddress(
                street_line_1=street_line_1,
                street_line_2=street_line_2,
                city=address_dict.get('PlaceName', ''),
                state=address_dict.get('StateName', ''),
                zipcode=address_dict.get('ZipCode', ''),
                name=address_dict.get('Recipient', '')
            )
        except usaddress.RepeatedLabelError as e:
            raise ValueError(f"Error parsing address: {str(e)}")

class ExtendedExtractedData(ExtractedData):
    other_info: ExtendedOrderInfo

    @classmethod
    def from_extracted_data(cls, extracted_data: ExtractedData):
        extended_order_info = ExtendedOrderInfo(**extracted_data.other_info.dict())
        extended_order_info.parse_addresses()
        return cls(
            product_table=extracted_data.product_table,
            other_info=extended_order_info
        )
    

def preprocess_address(address: str) -> str:
    # Check if address is None or empty
    if not address:
        return "" 
    
    # Split the address into lines
    address_lines = address.split('\n')
    
    # Define regex pattern for city, state, zip
    # This pattern is more flexible:
    # - It allows for full state names or abbreviations
    # - The comma after the city is optional
    # - It allows for spaces between zip code parts
    city_state_zip_pattern = re.compile(r'\b(?:[A-Za-z\s]+[,\s]+){1,2}(?:[A-Z]{2}|[A-Za-z\s]+)\s+\d{5}(?:[-\s]?\d{4})?\b', re.IGNORECASE)
    
    # Process lines to include the one containing city, state, and zip
    preprocessed_lines = []
    found_city_state_zip = False
    for line in address_lines:
        stripped_line = line.strip()
        preprocessed_lines.append(stripped_line)
        if city_state_zip_pattern.search(stripped_line):
            found_city_state_zip = True
            break

    # Remove duplicate lines
    preprocessed_lines = repeat_address_line_check(preprocessed_lines)
    
    if not found_city_state_zip:
        preprocessed_lines = repeat_address_line_check(address_lines)
    
    # Join the processed lines into a single string
    preprocessed_address = '\n'.join(preprocessed_lines)
    return preprocessed_address

def repeat_address_line_check(address_lines: list) -> list:
    for i, s1 in enumerate(address_lines):
        for _, s2 in enumerate(address_lines[i+1:]):

            if fuzz.token_set_ratio(s1.lower(), s2.lower()) > 85:
                logging.info(f"Address line removed: {s2}") 
                # remove s2 from original list
                address_lines.remove(s2)
    return address_lines


api_key = "75beba15a9f4447c9ff6a0511c261478"
version = "2024-02-15-preview"
azure_endpoint = "https://dynatect-openai-1.openai.azure.com/"


client_instructor = instructor.from_openai(AzureOpenAI(
    azure_endpoint = azure_endpoint,
    azure_deployment="gpt4o",
    api_key=api_key,
    api_version=version
    ))


def product_customer_llm_extraction(query: str) -> ExtractedData:
    """ Use pydantic to coerce a structured response from OpenAI 
        based on the query provided by the
    """

    messages = system_message_pdf_extraction + [{"role": "user", "content": query}]

    try:  
        # Extract structured data from natural language  
        part_info = client_instructor.chat.completions.create(  
            model="gpt-4o",  
            response_model=ExtractedData,  
            messages=messages,  
        )  
    except Exception as e:  
        print(f"An unexpected error occurred: {e}")   
        part_info = ExtractedData()  
  
    return part_info  