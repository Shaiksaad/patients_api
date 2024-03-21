import pandas as pd
import ibm_boto3
from ibm_botocore.client import Config
import io
import time

# Define the global variable df
df = None


def read_data_from_cos():
    api_key = 'TovXp3LfOt0Kx3qKoQ1N0L4caoazGiqNbhuD5kdpdjbI'
    service_instance_id = 'crn:v1:bluemix:public:cloud-object-storage:global:a/8e1b8a01ef9da7aa92b5d05e37608486:1ef37b3d-0d51-4343-9fa4-ea94890157d1::'
    cos_endpoint = 'https://s3.eu.cloud-object-storage.appdomain.cloud'
    cos_bucket_name = 'poc-obj'
    object_key = 'patient food.csv'  # Replace with the name of your CSV file

    # Connect to IBM Cloud Object Storage using ibm_boto3
    cos = ibm_boto3.client('s3',
                           ibm_api_key_id=api_key,
                           ibm_service_instance_id=service_instance_id,
                           config=Config(signature_version='oauth'),
                           endpoint_url=cos_endpoint)

    # Read the CSV file from COS as bytes
    response = cos.get_object(Bucket=cos_bucket_name, Key=object_key)
    data = response['Body'].read()

    # Create a pandas DataFrame from the CSV data
    global df
    df = pd.read_csv(io.BytesIO(data))


# Initialize the DataFrame when the code is loaded
read_data_from_cos()


def get_food_menu_by_patient_info(patient_name):
    global df
    matching_rows = df[df['Patient_name'] == patient_name]

    if not matching_rows.empty:
        patient_info = matching_rows.iloc[0]['Patient_of']
        food_menu = matching_rows.iloc[0]['Food_menu']
        return patient_info, food_menu


def initialize_order_id_counter():
    global order_id_counter
    # Initialize order_id_counter based on existing order IDs or start from 1
    # Example: order_id_counter = get_max_order_id() + 1
    order_id_counter = 1  # Start from 1 if there are no existing orders


# Call this function when your application starts
initialize_order_id_counter()


def generate_order_id():
    global order_id_counter
    order_id = f"OD{str(order_id_counter).zfill(3)}"  # Format the order ID
    order_id_counter += 1  # Increment the counter
    return order_id


def get_food_recommendation(patient_name, food_option):
    global df, df1

    # Initialize df1 as an empty DataFrame if it's not defined
    if 'df1' not in globals():
        df1 = pd.DataFrame()

    # Retrieve health_condition from the df DataFrame
    matching_rows = df[df['Patient_name'] == patient_name]

    if not matching_rows.empty:
        health_condition = matching_rows.iloc[0]['Patient_of']
        patient_info, food_menu = get_food_menu_by_patient_info(patient_name)

        # Check if the selected food_option is valid
        if food_option >= 1 and food_option <= len(food_menu.split('\n')):
            # Split the food_menu by newline character and get the selected option
            food_options = food_menu.split('\n')
            recommended_food = food_options[food_option - 1].strip()

            # Generate an order ID
            order_id = generate_order_id()

            # Get the current date and time
            date_time = time.strftime('%Y-%m-%d %H:%M:%S')

            # Set the status to "Pending"
            status = "Pending"

            # Create an order record and append it to the DataFrame
            order_record = {
                'OrderID': order_id,
                'PatientName': patient_name,
                'HealthCondition': health_condition,
                'FoodOrdered': recommended_food,
                'DateTime': date_time,
                'Status': status
            }

            if df1 is None:
                df1 = pd.DataFrame([order_record])
            else:
                df1 = df1.append(order_record, ignore_index=True)

            # Append the order record to the CSV file in COS
            append_order_to_cos(order_record)

            result_message = f"Order Placed Successfully"

        else:
            result_message = "Invalid food option. Please select a valid food option."

        if food_menu != "Patient not found":
            food_menu = food_menu.replace("\n", " ")

            if patient_info:
                result_message = f"{result_message} for {recommended_food}"
            else:
                result_message = f"Recommended food option is: {recommended_food}\n{result_message}"
        else:
            result_message = "Patient not found"

    else:
        result_message = "Patient not found"

    return {
        'result': result_message
    }


def append_order_to_cos(order_record):
    api_key = 'z5R8vfx83mvKXUvf7hi8GFHpZgnqkXHVUQJPhpVwoaIn'
    service_instance_id = 'crn:v1:bluemix:public:cloud-object-storage:global:a/8e1b8a01ef9da7aa92b5d05e37608486:1ef37b3d-0d51-4343-9fa4-ea94890157d1::'
    cos_endpoint = 'https://s3.eu.cloud-object-storage.appdomain.cloud'
    cos_bucket_name = 'poc-obj'
    object_key = 'food_order1.csv'  # Replace with the name of your CSV file

    # Connect to IBM Cloud Object Storage using ibm_boto3
    cos = ibm_boto3.client('s3',
                           ibm_api_key_id=api_key,
                           ibm_service_instance_id=service_instance_id,
                           config=Config(signature_version='oauth'),
                           endpoint_url=cos_endpoint)

    # Read the existing CSV file from COS
    response = cos.get_object(Bucket=cos_bucket_name, Key=object_key)
    existing_data = response['Body'].read()

    # Create a pandas DataFrame from the existing data
    df_existing = pd.read_csv(io.BytesIO(existing_data))

    # Concatenate the existing DataFrame with the new order_record
    df_updated = pd.concat([df_existing, pd.DataFrame([order_record])], ignore_index=True)

    # Write the updated DataFrame back to COS
    csv_data = df_updated.to_csv(index=False).encode('utf-8')
    cos.put_object(Bucket=cos_bucket_name, Key=object_key, Body=csv_data)


def check_food_recommendation(patient_name, food_name_text):
    # Check if the patient exists in the DataFrame
    patient_row = df[df['Patient_name'] == patient_name]

    if patient_row.empty:
        return {"result": "Patient not found"}

    food_menu = patient_row.iloc[0]['Food_menu']
    food_menu_list = [food.strip().lower() for food in food_menu.split('\n')]

    if food_name_text.lower() in food_menu_list:
        result_message = f"'{food_name_text}' is not recommended as per your health condition."
    else:
        result_message = f"'{food_name_text}' is not recommended for you. Please select food options from the recommended food menu only."

    return {"result": result_message}


def main(params):
    if 'patient_name' in params and 'food_option' in params:
        patient_name = params.get('patient_name')
        food_option = int(params.get('food_option'))

        return get_food_recommendation(patient_name, food_option)

    elif 'patient_name' in params and 'food_name_text' in params:
        patient_name = params.get('patient_name')
        food_name_text = params.get('food_name_text')

        return check_food_recommendation(patient_name, food_name_text)

    elif 'patient_name' in params:
        patient_name = params.get('patient_name')
        patient_info, food_menu = get_food_menu_by_patient_info(patient_name)

        if food_menu != "Patient not found":
            food_menu = food_menu.replace("\n", " ")

            if patient_info:
                result_message = f"As you are {patient_info} patient, the food menu available for you is: {food_menu}"
        else:
            result_message = "Patient not found"

    else:
        result_message = "Invalid input parameter"

    return {
        'result': result_message
    }
