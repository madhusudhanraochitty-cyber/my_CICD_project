# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC ##`extract_olds_jobs_dlt`
# MAGIC
# MAGIC Notebook to extract the latest definitions of the clds jobs and associated DLTs
# MAGIC
# MAGIC This Notebook uses the below DfE Data Engineering framework:
# MAGIC - cicd 
# MAGIC
# MAGIC |Version|Date|Who|Change|
# MAGIC |-|-|-|-|
# MAGIC |1.0|2024-11-25|CDB|Cloned from CLDS version|
# MAGIC

# COMMAND ----------

# MAGIC %pip install --quiet /Workspace/02-Pipelines/Data_Engineering/lib/dfe_framework_cicd-0.0.1-py3-none-any.whl

# COMMAND ----------

from pathlib import Path
# from dfe_framework.cicd import job_extract --
dbutils.import_notebook
from job_extract1 import export_jobs_pipelines
# COMMAND ----------


# get workspace files path to current Notebook - this is where the json files are
current_notebook_path = Path.cwd()
print('Current Notebook path is:\n\t{}'.format(current_notebook_path))

# assuming the notebook is somewhere in the repo, taking the first four levels of the Notebook path will give us the path to the root of the repo
repo_root_path = Path.cwd().parent
print('Root directory of the Repo is:\n\t{}'.format(repo_root_path))

# export the jobs and DLTs, writing the results into the repo
job_extract1.export_jobs_pipelines(
        control_file=str(current_notebook_path) + '/olds_jobs_dlts.json',
        job_output_path=str(repo_root_path) + '/bundle/jobs/',
        dlt_output_path='/tmp', # suppress creation of this for now #repo_root_path + 'bundle/dlt_pipelines/',
        clear_existing=True
)
print('Exporting pipelines complete!')



# COMMAND ----------

current_notebook_path = Path.cwd()
print('Current Notebook path is:\n\t{}'.format(current_notebook_path))

# COMMAND ----------

# Instead of: from job_extract1 import export_jobs_pipelines
current_notebook_path = Path.cwd()
# Call the notebook and pass parameters as a dictionary
result = dbutils.notebook.run(
    "/Workspace/Repos/madhu.sudhanraochitty@gmail.com/my_CICD_project/job_extract/job_extract1",
    60,  # timeout in seconds
    {
        "control_file": str(current_notebook_path) + '/olds_jobs_dlts.json',
        "job_output_path": str(repo_root_path) + '/bundle/jobs/',
        "dlt_output_path": '/tmp',
        "clear_existing": "True"
    }
)
print('Exporting pipelines complete!')
print(result)

# COMMAND ----------

from pathlib import Path

# Get paths
current_notebook_path = Path.cwd()
repo_root_path = current_notebook_path.parent  # Adjust if needed

print(f"Current Notebook path:\n{current_notebook_path}")
print(f"Repo Root path:\n{repo_root_path}")

# Run the other notebook and pass parameters
result = dbutils.notebook.run(
    "/Workspace/Repos/madhu.sudhanraochitty@gmail.com/my_CICD_project/job_extract/job_extract1",  # adjust if path differs
    120,  # timeout in seconds
    {
        "control_file": str(current_notebook_path) + '/olds_jobs_dlts.json',
        "job_output_path": str(repo_root_path) + '/bundle/jobs/',
        "dlt_output_path": '/tmp',
        "clear_existing": "True"
    }
)

print("✅ Exporting pipelines complete!")
print("Result returned from job_extract1:\n", result)

