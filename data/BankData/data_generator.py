# Garbage code generated with AI

import csv
import random
from faker import Faker

fake = Faker()

def generate_account_data(num_records, address_ids):
    account_data = []
    for _ in range(num_records):
        account = {}
        account['contact_id'] = fake.random_number(digits=8)
        account['contact_status'] = random.choice(['ACTIVE', 'DEACTIVATED', 'CLOSED', 'PENDING_ACTIVATION'])
        account['contact_first_name'] = fake.first_name()
        account['contact_last_name'] = fake.last_name()
        account['contact_email'] = fake.email()
        account['contact_phone'] = fake.phone_number()
        account['join_date'] = fake.date_of_birth(minimum_age=0, maximum_age=20)
        account['date_of_birth'] = fake.date_of_birth(minimum_age=1, maximum_age=99)
        address_id = random.choice(address_ids)
        account['contact_address_id'] = address_id
        address_ids.remove(address_id)
        account_data.append(account)
    return account_data

def generate_address_data(num_records):
    address_data = []
    for _ in range(num_records):
        address = {}
        address['address_id'] = fake.random_number(digits=8)
        address['address_country_code_iso2'] = fake.country_code(representation="alpha-2")
        address['address_city'] = fake.city()
        address['address_street'] = fake.street_address()
        address['address_postal_code'] = fake.postalcode()
        address_data.append(address)
    return address_data

def generate_checking_account_data(num_records, account_data):
    checking_account_data = []
    for _ in range(num_records):
        checking_account = {}
        checking_account['account_id'] = fake.random_number(digits=8)
        contact_id = random.choice(account_data)['contact_id']
        checking_account['contact_id'] = contact_id
        checking_account['account_balance'] = random.uniform(1000, 100000)
        checking_account_data.append(checking_account)
    return checking_account_data

def generate_securities_account_data(num_records, account_data):
    securities_account_data = []
    for _ in range(num_records):
        securities_account = {}
        securities_account['account_id'] = fake.random_number(digits=8)
        contact_id = random.choice(account_data)['contact_id']
        securities_account['contact_id'] = contact_id
        securities_account['code_isin'] = fake.random_number(digits=6)
        securities_account['account_balance'] = random.uniform(1000, 100000)
        securities_account_data.append(securities_account)
    return securities_account_data

def generate_insurance_account_data(num_records, account_data):
    insurance_account_data = []
    for _ in range(num_records):
        insurance_account = {}
        insurance_account['account_id'] = fake.random_number(digits=8)
        contact_id = random.choice(account_data)['contact_id']
        insurance_account['contact_id'] = contact_id
        insurance_account['insurance_type'] = random.choice(['Car Insurance', 'House Insurance', 'Health Insurance'])
        insurance_account_data.append(insurance_account)
    return insurance_account_data

num_accounts = 150000
num_checking_accounts = 300000
num_securities_accounts = 120000
num_insurance_accounts = 100000
num_addresses = num_accounts

address_data = generate_address_data(num_addresses)
account_data = generate_account_data(num_accounts, [address['address_id'] for address in address_data])
checking_account_data = generate_checking_account_data(num_checking_accounts, account_data)
securities_account_data = generate_securities_account_data(num_securities_accounts, account_data)
insurance_account_data = generate_insurance_account_data(num_insurance_accounts, account_data)

# Write data to CSV files
with open('contact.csv', 'w', newline='') as csvfile:
    fieldnames = ['contact_id', 'contact_status', 'contact_first_name', 'contact_last_name', 'contact_email', 'contact_phone', 'contact_address_id', 'join_date', 'date_of_birth']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(account_data)

# Update the generation functions to ensure matching Contact_ID in other files
checking_account_data = generate_checking_account_data(num_checking_accounts, account_data)
securities_account_data = generate_securities_account_data(num_securities_accounts, account_data)
insurance_account_data = generate_insurance_account_data(num_insurance_accounts, account_data)

with open('checking_account.csv', 'w', newline='') as csvfile:
    fieldnames = ['account_id', 'contact_id', 'account_balance']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(checking_account_data)

with open('security_account.csv', 'w', newline='') as csvfile:
    fieldnames = ['account_id', 'contact_id', 'code_isin', 'account_balance']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(securities_account_data)

with open('insurance_account.csv', 'w', newline='') as csvfile:
    fieldnames = ['account_id', 'contact_id', 'insurance_type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(insurance_account_data)

with open('adress.csv', 'w', newline='') as csvfile:
    fieldnames = ['address_id', 'address_country_code_iso2', 'address_city', 'address_street', 'address_postal_code']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(address_data)

def generate_generic_account_data(account_data, checking_account_data, securities_account_data, insurance_account_data):
    generic_account_data = []
    
    # Combine Account IDs from different sources
    account_ids = [account['contact_id'] for account in account_data]
    checking_account_ids = [account['account_id'] for account in checking_account_data]
    securities_account_ids = [account['account_id'] for account in securities_account_data]
    insurance_account_ids = [account['account_id'] for account in insurance_account_data]
    
    all_account_ids = set(account_ids + checking_account_ids + securities_account_ids + insurance_account_ids)
    
    # Generate Generic Account data
    for account_id in all_account_ids:
        generic_account = {}
        generic_account['account_id'] = account_id
        
        # Set the ACCOUNT_TYPE based on the source
        if account_id in checking_account_ids:
            generic_account['account_type'] = 'Checking'
        elif account_id in securities_account_ids:
            generic_account['account_type'] = 'Security'
        elif account_id in insurance_account_ids:
            generic_account['account_type'] = 'Insurance'
        else:
            generic_account['account_type'] = random.choice(['CDS', 'Basic', 'Salary'])
        
        generic_account_data.append(generic_account)
    
    return generic_account_data

# Generate Generic Account data
generic_account_data = generate_generic_account_data(account_data, checking_account_data, securities_account_data, insurance_account_data)

# Write data to Generic Account CSV file
with open('account.csv', 'w', newline='') as csvfile:
    fieldnames = ['account_id', 'account_type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(generic_account_data)