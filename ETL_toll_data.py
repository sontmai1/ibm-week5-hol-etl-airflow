# import the libraries 
from datetime import timedelta 
# The DAG object; we'll need this to instantiate a DAG 
from airflow import DAG 
# Operators; we need this to write tasks! 
from airflow.operators.bash_operator import BashOperator 
# This makes scheduling easy 
from airflow.utils.dates import days_ago 

# defining DAG arguments 

# You can override them on a per-task basis during operator initialization 
default_args = { 

    'owner': 'Dummy Name', 
    'start_date': days_ago(0), 
    'email': ['dummy@somemail.com'], 
    'email_on_failure': True, 
    'email_on_retry': True, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
} 

# defining the DAG 

# define the DAG 
dag = DAG( 

    'ETL_toll_data', 
    default_args=default_args, 
    description='Apache Airflow Final Assignment', 
    schedule_interval=timedelta(days=1), 
) 

# define the tasks 

# define the task 'unzip_data' 
unzip_data = BashOperator( 
    task_id='unzip_data', 
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment', 
    dag=dag, 
) 

# define the task 'extract' 
extract_data_from_csv = BashOperator( 
    task_id='extract_data_from_csv', 
    bash_command='cut -f1-4 -d"," /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv', 
    dag=dag, 
) 

# define the task 'extract_data_from_tsv' 
extract_data_from_tsv = BashOperator( 
    task_id='extract_data_from_tsv', 
    bash_command=""" 
        cut -f5-7 -d '\t' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | 
        tr '\t' ',' |
        tr "[:blank:]" ","> /home/project/airflow/dags/finalassignment/tsv_data.csv 
    """, 
    dag=dag, 
) 

# bash_command='cut -f5-7 -d"\t" /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv', 

# define the task 'extract_data_from_fixed_width' 
extract_data_from_fixed_width = BashOperator( 

    task_id='extract_data_from_fixed_width', 
    bash_command=""" 
        cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | 
        tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv 
    """, 
    dag=dag, 
) 

# bash_command='cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv', 

# cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv 

# paste -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv | tr -d '\r' > /home/project/airflow/dags/finalassignment/temp.csv 

# paste -d ',' /home/project/airflow/dags/finalassignment/temp.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/test.csv 

# bash_command='cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv', 

# bash_command='cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv', 

 

#define the task 'consolidate_data' 

consolidate_data = BashOperator( 
    task_id='consolidate_data', 
    bash_command=""" 
        paste -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv 
    """, 
    dag=dag, 
) 

# bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv', 

# bash_command=""" 

#         paste -s -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv > /home/project/airflow/dags/finalassignment/temp_extracted_data.csv 

#         paste /home/project/airflow/dags/finalassignment/temp_extracted_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv | 

#         tr ['\t'] ',' > /home/project/airflow/dags/finalassignment/extracted_data.csv 

#     """, 

# paste -d ',' /home/project/airflow/dags/finalassignment/temp_extracted_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/transform_data.csv 

# define the task 'transform_data' 

transform_data = BashOperator( 
    task_id='transform_data', 
    bash_command=""" 
        cut -d , -f 1-9 /home/project/airflow/dags/finalassignment/extracted_data.csv | 
        tr [:lower:] [:upper:] > /home/project/airflow/dags/finalassignment/transform_data.csv 
    """, 
    dag=dag, 
) 

# task pipeline 

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data 

# define the task 'transform_data' 
# transform_data = BashOperator( 
#     task_id='transform_data', 
#     bash_command='cut -d ',' -f 1,2,3,4 | tr '[:lower:]' '[:upper:]' | paste -d ',' - - - - < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt', 
#     dag=dag, 
# ) 

# bash_command='cut -d ',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr '[:lower:]' '[:upper:]' | paste -d ',' - - - - > /home/project/airflow/dags/finalassignment/transform_data.csv', 

# transform_data = BashOperator( 
#     task_id='transform_data', 
#     bash_command=""" 
#         cut -d , -f 1,2,3,4 /home/project/airflow/dags/finalassignment/extracted_data.csv | 
#         tr [:lower:] [:upper:] | 
#         paste -d , - - - - > /home/project/airflow/dags/finalassignment/transform_data.csv 
#     """, 
#     dag=dag, 
# ) 
