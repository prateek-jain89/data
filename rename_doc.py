import json,os

base_dir = '/home/prateek.jain/lab/data/2021_02_17'
resp_submit_file = 'resp_submit.json'
db_details_file = 'db_details.json'

with open(f'{base_dir}/{db_details_file}') as f:
    data = json.load(f)


with open(f"{base_dir}/{resp_submit_file}", "r") as fs:
    submission_data = json.load(fs)
    
# len(submission_data)

ddb_table_name = 'nv-dynamodb-hubbleapi-prod-logs'

job_mapping = []

for item_d in data['translation_success']:
    responses = item_d['Responses']
    for item_r in responses[f'{ddb_table_name}']:
        fetch_id_r = item_r['fetch_id']
        
        # file_name_resp = item_r['file_path_translated']['k'].rsplit('/', 1)[1]
        file_name_resp = item_r['file_path_translated_document']['k'].rsplit('/', 1)[1]
        
        for item_sd in submission_data:
            fetch_id_sd = item_sd['fetch_id']
            file_name_org = item_sd['file_name']
            
            if fetch_id_sd == fetch_id_r:
                job_mapping.append({
                    "fetch_id": fetch_id_sd ,
                    "file_name_org": file_name_org,
                    "file_name_resp": file_name_resp
                })

            
# len(job_mapping), job_mapping[0]


base_dir_result = f'{base_dir}/result_docx'
# print(base_dir_result)
for item in job_mapping:
    try:
        os.rename(
            f"{base_dir_result}/{item['file_name_resp']}", 
            # f"{base_dir_result}/{item['file_name_org'].rsplit('.', 1)[0]}.json",
            f"{base_dir_result}/{item['file_name_org'].rsplit('.', 1)[0]}.docx"
        ) 
        
    except Exception as e:
        print(e)