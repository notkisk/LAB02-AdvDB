import os
import struct
import re

PAGE_SIZE = 4096

def create_heap_file(file_name: str):
    with open(file_name, 'wb') as f:
        pass

def read_page(file_name: str, page_number: int) -> bytes:
    file_size = os.path.getsize(file_name)
    last_page_number = file_size // PAGE_SIZE - 1
    if page_number > last_page_number:
        raise ValueError(f"Page {page_number} does not exist in the file.")
    with open(file_name, 'rb') as f:
        f.seek(page_number * PAGE_SIZE)
        page_data = f.read(PAGE_SIZE)
    return page_data

def append_page(file_name: str, page_data: bytes):
    if len(page_data) != PAGE_SIZE:
        raise ValueError(f"Page data must be exactly {PAGE_SIZE} bytes.")
    with open(file_name, 'ab') as f:
        f.write(page_data)

def write_page(file_name: str, page_number: int, page_data: bytes):
    file_size = os.path.getsize(file_name)
    last_page_number = file_size // PAGE_SIZE - 1
    if page_number > last_page_number:
        raise ValueError(f"Page {page_number} does not exist in the file.")
    if len(page_data) != PAGE_SIZE:
        raise ValueError(f"Data must be exactly {PAGE_SIZE} bytes long.")
    with open(file_name, 'r+b') as f:
        f.seek(page_number * PAGE_SIZE)
        f.write(page_data)

def calculate_free_space(page_data: bytes) -> int:
    free_space_offset = int.from_bytes(page_data[4094:4096], byteorder='big')
    slot_count = int.from_bytes(page_data[4092:4094], byteorder='big')
    used_space = free_space_offset + slot_count * 4 + 4
    return PAGE_SIZE - used_space


def insert_record_data_to_page_data(page_data: bytes, record_data: bytes) -> bytes:
    record_length = len(record_data)
    page_data_barray = bytearray(page_data)
    slot_count = int.from_bytes(page_data_barray[4092:4094], byteorder='big')
    free_space_offset = int.from_bytes(page_data_barray[4094:4096], byteorder='big')

    if free_space_offset == 0 and slot_count == 0:
        free_space_offset = 0

    free_space = PAGE_SIZE - (free_space_offset + slot_count * 4 + 4)
    if free_space < record_length + 4:
        raise ValueError("Not enough free space in the page to insert the record.")

    page_data_barray[free_space_offset:free_space_offset + record_length] = record_data

    slot_entry_offset = 4092 - ((slot_count + 1) * 4)
    page_data_barray[slot_entry_offset:slot_entry_offset + 2] = free_space_offset.to_bytes(2, 'big')
    page_data_barray[slot_entry_offset + 2:slot_entry_offset + 4] = record_length.to_bytes(2, 'big')

    page_data_barray[4092:4094] = (slot_count + 1).to_bytes(2, 'big')
    page_data_barray[4094:4096] = (free_space_offset + record_length).to_bytes(2, 'big')

    return bytes(page_data_barray)


def insert_record_to_file(file_name: str, record_data: bytes):
    file_size = os.path.getsize(file_name) if os.path.exists(file_name) else 0
    total_pages = file_size // PAGE_SIZE
    inserted = False
    for page_number in range(total_pages):
        page_data = read_page(file_name, page_number)
        free_space = calculate_free_space(page_data)
        if free_space >= len(record_data) + 4:
            updated_page = insert_record_data_to_page_data(page_data, record_data)
            write_page(file_name, page_number, updated_page)
            inserted = True
            break
    if not inserted:
        page_data = b'\x00' * PAGE_SIZE
        updated_page = insert_record_data_to_page_data(page_data, record_data)
        append_page(file_name, updated_page)

def get_record_from_page(page_data: bytes, record_id: int) -> bytes:
    slot_count = int.from_bytes(page_data[4092:4094], 'big')
    if record_id >= slot_count:
        raise ValueError("Record ID out of range")
    
    slot_entry_offset = 4092 - 4 * (record_id + 1)
    
    record_offset = int.from_bytes(page_data[slot_entry_offset:slot_entry_offset+2], 'big')
    record_length = int.from_bytes(page_data[slot_entry_offset+2:slot_entry_offset+4], 'big')
    
    return page_data[record_offset:record_offset+record_length]

def get_record_from_file(file_name: str, page_number: int, record_id: int) -> bytes:
    page_data = read_page(file_name, page_number)
    return get_record_from_page(page_data, record_id)

def get_all_records_from_page(page_data: bytes) -> list[bytes]:
    records = []
    slot_count = int.from_bytes(page_data[4092:4094], byteorder='big')
    for record_id in range(slot_count):
        records.append(get_record_from_page(page_data, record_id))
    return records

def get_all_records_from_file(file_name: str) -> list[bytes]:
    file_size = os.path.getsize(file_name)
    total_pages = file_size // PAGE_SIZE
    records = []
    for page_number in range(total_pages):
        page_data = read_page(file_name, page_number)
        records.extend(get_all_records_from_page(page_data))
    return records

def encode_record(record_dict, table_name, schema) -> bytes:
    table_info = next((t for t in schema if t['table_name'] == table_name), None)
    if not table_info:
        raise ValueError(f"Table {table_name} not found in schema")
    
    encoded = b''
    for field in table_info['fields']:
        fname = field['name']
        ftype = field['type']
        val = record_dict[fname]
        
        if ftype == 'int':
            encoded += struct.pack('>i', int(val))
        elif ftype == 'float':
            encoded += struct.pack('>f', float(val))
        elif ftype.startswith('char'):
            length = int(ftype[5:-1])
            s = str(val).encode('utf-8')
            encoded += struct.pack(f'{length}s', s)
        elif ftype.startswith('varchar'):
            s = str(val).encode('utf-8')
            encoded += struct.pack('B', len(s)) + s
    return encoded

def decode_record(record_bytes, table_name, schema) -> dict:
    table_info = next((t for t in schema if t['table_name'] == table_name), None)
    if not table_info:
        raise ValueError(f"Table {table_name} not found in schema")
    
    decoded = {}
    offset = 0
    for field in table_info['fields']:
        fname = field['name']
        ftype = field['type']
        
        if ftype == 'int':
            decoded[fname] = struct.unpack_from('>i', record_bytes, offset)[0]
            offset += 4
        elif ftype == 'float':
            decoded[fname] = struct.unpack_from('>f', record_bytes, offset)[0]
            offset += 4
        elif ftype.startswith('char'):
            length = int(ftype[5:-1])
            s = struct.unpack_from(f'{length}s', record_bytes, offset)[0]
            decoded[fname] = s.decode('utf-8').rstrip('\x00')
            offset += length
        elif ftype.startswith('varchar'):
            l = struct.unpack_from('B', record_bytes, offset)[0]
            offset += 1
            s = struct.unpack_from(f'{l}s', record_bytes, offset)[0]
            decoded[fname] = s.decode('utf-8')
            offset += l
            
    return decoded

def insert_structured_record(table_name, schema, record_dict):
    table_info = next((t for t in schema if t['table_name'] == table_name), None)
    if not table_info:
        raise ValueError(f"Table {table_name} not found in schema")
        
    file_name = table_info['file_name']
    if not os.path.exists(file_name):
        create_heap_file(file_name)
        
    record_bytes = encode_record(record_dict, table_name, schema)
    insert_record_to_file(file_name, record_bytes)

def read_all_structured_records(table_name, schema):
    table_info = next((t for t in schema if t['table_name'] == table_name), None)
    if not table_info:
        raise ValueError(f"Table {table_name} not found in schema")
        
    file_name = table_info['file_name']
    if not os.path.exists(file_name):
        return []
        
    raw_records = get_all_records_from_file(file_name)
    return [decode_record(r, table_name, schema) for r in raw_records]

def parse_select_query(query, schema) -> dict:
    q = query.strip()
    match = re.match(r'SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?', q, re.IGNORECASE)
    if not match:
        raise ValueError("Invalid SELECT query")
    
    fields_str, table_name, condition_str = match.groups()
    
    fields = [f.strip() for f in fields_str.split(',')]
    if fields == ['*']:
        table_info = next((t for t in schema if t['table_name'] == table_name), None)
        if table_info:
            fields = [f['name'] for f in table_info['fields']]
            
    result = {
        "fields": fields,
        "table": table_name,
        "condition": None
    }
    
    if condition_str:
        cond_match = re.match(r'(\w+)\s*=\s*(.+)', condition_str)
        if cond_match:
            c_field, c_value = cond_match.groups()
            c_value = c_value.strip()
            if c_value.startswith("'") and c_value.endswith("'"):
                c_value = c_value[1:-1]
            elif c_value.isdigit():
                c_value = int(c_value)
            else:
                try:
                    c_value = float(c_value)
                except:
                    pass
            result["condition"] = {"field": c_field, "value": c_value}
            
    return result

def parse_insert_query(query, schema) -> dict:
    match = re.match(r'INSERT\s+INTO\s+(\w+)\s*\((.+)\)\s*VALUES\s*\((.+)\)', query, re.IGNORECASE)
    if not match:
        raise ValueError("Invalid INSERT query")
        
    table_name, fields_str, values_str = match.groups()
    fields = [f.strip() for f in fields_str.split(',')]
    
    values = []
    for v in values_str.split(','):
        v = v.strip()
        if v.startswith("'") and v.endswith("'"):
            values.append(v[1:-1])
        elif v.isdigit():
            values.append(int(v))
        else:
            try:
                values.append(float(v))
            except:
                values.append(v)
                
    return {
        "table": table_name,
        "fields": fields,
        "values": values
    }

def execute_query(query, schema):
    if query.strip().upper().startswith("SELECT"):
        parsed = parse_select_query(query, schema)
        records = read_all_structured_records(parsed['table'], schema)
        
        if parsed['condition']:
            cond = parsed['condition']
            records = [r for r in records if r.get(cond['field']) == cond['value']]
            
        result = []
        for r in records:
            result.append({k: r[k] for k in parsed['fields'] if k in r})
        return result
        
    elif query.strip().upper().startswith("INSERT"):
        parsed = parse_insert_query(query, schema)
        record = dict(zip(parsed['fields'], parsed['values']))
        insert_structured_record(parsed['table'], schema, record)
        return "Inserted successfully"
    else:
        raise ValueError("Unsupported query type")