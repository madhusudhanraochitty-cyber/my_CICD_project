# Databricks notebook source
import json
import glob
import os
import re
import yaml

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# import json
# import glob
# import os
# import re
# import yaml

# from databricks.sdk import WorkspaceClient

# def _process_environment_override_item(env_dict:dict, parsed_path, override_key, override_value) -> None:
#     """Add a new item into the environment override dictionary

#     Inserts a new key=value pair into the environment dictionary under the specified path.  For example, if the path is
#     ["hello", "hula", "hoop"] and the key value pair is "cat"="white", the dictionary will be updated to include:
#     {
#         "hello": {
#             "hula": {
#                 "hoop": {
#                     "cat": "white"
#                 }
#             }
#         }
#     }

#     The intermediate level dictionaries are created if they don't already exist.  Where an entry in the parsed_path is itself a "key=value"
#     string, this is assumed that this is a list of dictionaries not a dictionary with the list item being identified by containnig an item
#     where the key value match the key=value from the path.


#     Args:
#         env_dict - the dictionary in which to insert the item
#         parsed_path - list of the path to the item
#         override_key, override_value - the key value pair to insert
#     """

#     parent_dict = None
#     parent_key = None
#     current_dict = env_dict

#     for path_element in parsed_path:
#         # = means it refers to a list entry
#         if '=' in path_element:
#             # change the parent type to be a list if it is a dictionary
#             if type(parent_dict[parent_key]) is dict:
#                 parent_dict[parent_key] = []
            
#             # find out the name of the tag we are looking for
#             list_item_key, list_item_value = path_element.split('=')
#             found_dict = None

#             # search for the existing item
#             for list_item in parent_dict[parent_key]:
#                 if list_item[list_item_key] == list_item_value:
#                     # found it?
#                     found_dict = list_item
#                     break
            
#             # move to work on the dictionary in this array item
#             if found_dict:
#                 current_dict = found_dict
#             else:
#                 # create a new dictionary and insert it into the list
#                 new_dict = {list_item_key: list_item_value}              
#                 parent_dict[parent_key].append(new_dict)
                    
#                 # then move into work on the new item
#                 current_dict = new_dict

#             # clear away the parent information - this process assumes there will never be a list immediately within a list
#             parent_dict = None
#             parent_key = None

#         else:
#             # not seen this before?  create an empty dictionary
#             if path_element not in current_dict:
#                 current_dict[path_element] = {}

#             # move down the path
#             parent_dict = current_dict
#             parent_key = path_element
#             current_dict = current_dict[path_element]

#     # store the overridden value
#     current_dict[override_key] = override_value

# def _process_environment_override(tag_key:str, item_overrides:dict, parsed_path:list, environment_override:dict):
#     """process an environment override dictionary in the config json

#     The environment override replaces a simple value such as "${cluster_type}" with a dictionary containing key:value pair
#     where the key is the environment name and the value is the value that should apply in that environment.

#     This adds the necessary entries into the environment_override dictionary for the one tag.

#     Args:
#         tag_key: the key being overriden
#         item_overrides: dictionary containing an item for each environment being overridden
#         parsed_path: the path within the job json where the tag_key is 
#         environment_override: working dictionary that gets the new values added into it
#     """

#     for env, override_value in item_overrides.items():
#         # validate that the environment is a valid one
#         if env not in ('dev', 'test', 'SIT', 'preprod', 'prod'):
#             raise ValueError(f'Environment {env} is not known when generating environment override')

#         # if the first override for this environment, create an empty dictionary
#         if env not in environment_override:
#             environment_override[env] = {}

#         _process_environment_override_item(environment_override[env], parsed_path, tag_key, override_value)

# def _calculate_substitute_value(rule_value:str, existing_value:str, predefined_variables:dict, build_target:str):
#     """calculate the new value of a key based in the rule

#     The rule_value can be one of the following patterns:
#         'literal string' - any value not starting with $, # or ~ is treated as a fixed value to be written out unmodified
#         '${variable}' - replaced with a string variable in the target format
#         '#{variable}' - replaced with a numeric variable in the target format
#         any type that isn't a string - assumed to be a literal value

#     If the rule starts with a ~, then a regular expression followed by a ~ and one of the above rules

#         '~regexp~${variable}' - replace any part of the string that matches the regular expression with

#     Before writing a string varaible, a check is made of the predefined_variables - if there is a definition there, the value from predefined_variables
#     is written out.  This is used to allow ${job_name} to be replaced with the job name.

#     Args:
#         rule_value - value as read from the rule - see below
#         existing_value - the existing value before substitution
#         predefined_varaibles - dictionary of variables with fixed values - e.g. job_name
#         build_target - either 'json' or 'yaml' to indicate what syntax to generate

#     Returns:
#         value to be substituted

#     """
#     # currently support yaml (databricks asset bundles) and json (jobs API)
#     if build_target not in ('yaml', 'json'):
#         raise ValueError(f'build_target must be yaml or json')

#     # if an integer or boolean, return that value immediately - no rules to apply
#     if type(rule_value) is not str:
#         return rule_value

#     if rule_value.startswith('~'):
#         # extract the regular expression
#         split_rule = rule_value.split('~')
#         if len(split_rule) != 3:
#             raise ValueError(f'Rule value "{rule_value}" does not contain a regular expression and replacement value')

#         # record the regular expression for use later
#         regexp_search = split_rule[1]
#         rule_value = split_rule[2]
#     else:
#         regexp_search = None

#     # string variable or numeric variable?
#     if (rule_value.startswith('${') or rule_value.startswith('#{')) and rule_value.endswith('}'):
#         # format the variable depending on if we are targetting 'new' yaml or 'old' json
#         variable_value_raw = rule_value[2:-1]

#         # look-up a replacement value - if it's there return the replacement value immediately
#         if variable_value_raw in predefined_variables:
#             return predefined_variables[variable_value_raw]

#         if build_target=='yaml':
#             # for yaml, replace with a variable in format expected by asset bundles
#             new_value = '${var.' + variable_value_raw + '}'
#         else:
#             # for json, use our custom tokens
#             new_value = '^[^' + variable_value_raw + '^]^'

#         # numeric variables are written out with special 'magic' tags #[# #]#.  This is because the variable is a string
#         # it can only be written out as a string type - in the resulting json or yaml it has a string type.  This process
#         # does a global search and replace to remove "#[# and #]#" before writing out the file.  Although this results in
#         # invalid json, because the enclosed variable is replaced before the json is parsed there is no problem
#         if rule_value.startswith('#'):
#             new_value = '#[#' + new_value + '#]#'

#     # literal string value?
#     else:
#         new_value = rule_value

#     # finally if there is a regular expression search and replace to do to the existing value, do that
#     if regexp_search:
#         new_value = re.sub(regexp_search, new_value, existing_value)

#     return new_value

# def _tokenise_dictionary(job_defn_dict:dict, token_rules:dict, predefined_variables:dict, parsed_path:list, substituion_logs:list, build_target:str, environment_override:dict) -> None:
#     """Apply tokenisation to a dictionary containing a job definition or a child dictionary somehwere in that definition

#     Args:
#         job_defn_dict: Dictionary containing the Databricks job definition as returned from the API or a child dictionary within it
#         token_rules: Dictionary of token rules to apply
#         predefined_variables: Dictionary containing fixed values to apply instead of generating a variable
#         parsed_path: List recording dictionary key names or list entry key values as the dictionary is walked through
#         substitution_logs: list to add records of any substitutions made
#         environment_override: dictionary to build up any environment overrides when processing yaml files
#     """

#     # iterate through each rule
#     for tag, value in token_rules.items():

#         # a tag can be prefixed with + to create the entry if it does not exist
#         # or - to remove the entry
#         if tag[0] in ('+', '-'):
#             clean_tag = tag[1:]
#         else:
#             clean_tag = tag

#         # extract the value of the tag before any substitution is made - is is primarily for reporting what got changed
#         existing_value = job_defn_dict.get(clean_tag, None)

#         if type(value) is dict and 'environment_override' in value:
#             # for json we substitute the variable as this format does not support the generating of overrides in the
#             # job definition json
#             if build_target=='json':
#                 value = value['json_variable']
#             else:
#                 # add the value(s) to the environment specific override if the value is found or it is an addative operation
#                 if clean_tag in job_defn_dict or clean_tag[0]=='+':
#                     _process_environment_override(clean_tag, value['environment_override'], parsed_path, environment_override)

#                 # optionally allow the default value to be set - this is used in cases where either we only want one environment to be different
#                 # or where there is some odd behaviour in how asset bundles handles the override - for example for DLT development mode,
#                 # true can override false but false cannot override true - therefore need to force the value to false using the default setting
#                 if 'default' in value:
#                     value = value['default']
#                 else:
#                     #  otherwise set the value to none to ensure the value currently in dev is unaltered
#                     value = None
                

#         # branch handling based on the type of the value in the key:value pair in the rule
#         if type(value) is str or type(value) is bool:
#             # if the rule is in the format:
#             #       'pause_status': '${SchedulePausedStatus}'
#             # then replace the value of 'pause_status' in the job json definition with the value '${SchedulePausedStatus}'
#             new_value = None

#             if tag[0] == '-':
#                 # if the request is to remove the tag then the value in the rule is ignored
#                 if clean_tag in job_defn_dict:
#                     del job_defn_dict[clean_tag]
#             elif clean_tag in job_defn_dict or tag[0]=='+':
#                 # if the tag exists in the job dictionary then replace it
#                 # if the tag rule is prefixed with + then add the key:value if not already there, update if it is
#                 new_value = _calculate_substitute_value(value, existing_value, predefined_variables, build_target)
#                 job_defn_dict[clean_tag] = new_value

#             # store the log of what got changed
#             if existing_value is not None or new_value is not None:
#                 full_json_path = '/' + '/'.join(parsed_path) + f'/{clean_tag}'
#                 substituion_logs.append( (predefined_variables['job_name'], full_json_path, existing_value, new_value ))

#         elif type(value) is list:
#             # if the rule is in the format:
#             #       'on_start': [ '${JobAlertEmailAddress}' ],
#             # then process each item in the list.  Assumed that each item is a 
#             # add the task

#             # exit out if trying to delete a tag using this method - this should be done using a simpele string
#             if tag[0]=='-':
#                 raise ValueError('Deleting tags using a list type is not supported')

#             if clean_tag in job_defn_dict or tag[0]=='+':
#                 lst_replace = []
                
#                 for rule_list_entry in value:
#                     if type(rule_list_entry) is not str:
#                         raise ValueError('Lists containing types other than string are not currently supported')

#                     lst_replace.append(_calculate_substitute_value(rule_list_entry, None, predefined_variables, build_target))

#                 job_defn_dict[clean_tag] = lst_replace

#                 # record details of the substitution made
#                 full_json_path = '/' + '/'.join(parsed_path) + f'/{clean_tag}'
#                 substituion_logs.append( (predefined_variables['job_name'], full_json_path, existing_value, lst_replace) )

#         elif type(value) is dict:
#             # if the rule is in the format:
#             #      'job_clusters': { <another dictionary }
#             # then recursively apply the rules in the dictionary to the value of the dictionary

#             # if the tag is addative - if the tag doesn't exist, create one
#             if tag[0] == '+' and clean_tag not in job_defn_dict:
#                 job_defn_dict[clean_tag] = {}

#             if clean_tag in job_defn_dict:
#                 child_item = job_defn_dict[clean_tag]
#                 if type(child_item) is dict:
#                     _tokenise_dictionary(child_item, value, predefined_variables, parsed_path + [ clean_tag ], substituion_logs, build_target, environment_override)
#                 elif type(child_item) is list:
#                     # assume this is a list of dictionaries.  We apply the rule to each entry in the list OR selectively if there
#                     # is a key restriction

#                     # extract a list of the rules that apply to all items in the list
#                     all_items_rules = {}
#                     for rule_item, rule_value in value.items():
#                         # only keep the ones that don't contain a regular expression filter
#                         if '=' not in rule_item:
#                             all_items_rules[rule_item] = rule_value

#                     # loop through each item in the list
#                     for dict_list_entry in child_item:
#                         # make sure this is a dictionary
#                         if type(dict_list_entry) is not dict:
#                             raise ValueError('Can only process lists containing dictionaries')

#                         # extract the 'list_entry_name' from the item
#                         if clean_tag=='tasks':
#                             list_entry_name = dict_list_entry.get('task_key', None)
#                             list_entry_key = 'task_key'
#                         elif clean_tag=='job_clusters':
#                             list_entry_name = dict_list_entry.get('job_cluster_key', None)
#                             list_entry_key = 'job_cluster_key'
#                         elif clean_tag=='clusters':
#                             list_entry_name = dict_list_entry.get('label', None)
#                             list_entry_key = 'label'
#                         elif clean_tag=='libraries':
#                             list_entry_name = None
#                             list_entry_key = None
#                         else:
#                             raise Exception(f'Unknown Id tag in list {clean_tag}')

#                         if list_entry_name:
#                             add_to_list = [ clean_tag, list_entry_key + '=' + list_entry_name ]
#                         else:
#                             add_to_list = [ clean_tag ]

#                         # apply any rules that apply to all entries in the list
#                         if all_items_rules:
#                             _tokenise_dictionary(dict_list_entry, all_items_rules, predefined_variables, parsed_path + add_to_list, substituion_logs, build_target, environment_override)

#                         # next check and apply rules that are specific 
#                         for rule_item, rule_value in value.items():
#                             # does the rule item contain a regular expression?
#                             if '=' in rule_item:
#                                 lookup_key, lookup_regexp = rule_item.split('=')
#                                 actual_value = dict_list_entry.get(lookup_key, None)
#                                 if re.fullmatch(lookup_regexp, actual_value):
#                                     # if this rule applies to this toke
#                                     _tokenise_dictionary(dict_list_entry, rule_value, predefined_variables, parsed_path + add_to_list, substituion_logs, build_target, environment_override)

#                 else:
#                     raise ValueError('Unable to apply dictionary rules to type that is not dictionary or list')

#         elif value is None:
#             # none is supplied to force a skip of this logic when using an environment specific dictionary for yaml
#             pass
#         else:
#             # any other type is not currently supported
#             raise ValueError('Unhandled data type in job update ruleset')

# def _parameterise_job_definition(job_defn_dict:dict, all_token_rules:dict, substituion_logs:list, build_target:str, environment_override:dict) -> None:
#     """Apply all applicable tokenisation rukes to the Databricks job definition Python dictionary

#     Check each rule in the list of rules to see if it applies to the job.  This is based on the job name matching the 
#     regular expression in the all_token_rules dictionary

#     Args:
#         job_defn_dict: Dictionary containing the Databricks job definition as returned from the API
#         all_token_rules: Dictionary of token rules.  The key is a regular expression to match against the job name
#           with the value being a dictionary containing the tokenisation rules that apply.
#         substitution_logs: list to write log of substitutions made into
#         build_target: should the output be json or yaml
#         environment_override: dictionary to build up any environment overrides when processing yaml files
#     """
#     for job_regexp, token_rules in all_token_rules.items():
#         # extract the job name from the dictionary
#         job_name = job_defn_dict['name']

#         # test each possible setting to see if it applies to this job
#         if re.fullmatch(job_regexp, job_name):
#             # make the variable ${job_name} resolve directly to the current job name rather than expecting
#             # a variable called job_name to be supplied
#             predefined_variables = { 'job_name': job_name, 'dlt_name': 'dlt.' + job_name }

#             # call the tokenise function to perform the tokenisation requested in the token_rules
#             _tokenise_dictionary(job_defn_dict, token_rules, predefined_variables, parsed_path=[], substituion_logs=substituion_logs, build_target=build_target, environment_override=environment_override)

# def _extract_candidate_job_list(tag_name:str, tag_value_regexp:str) -> dict:
#     """Extract a dictionary of job_id={job_settings} key value pairs for all jobs that are tagged as required.

#     Args:
#         tag_name: Name of tag to extract the value of
#         tag_value_regexp: Regular expression to match against the value of tag_name

#     Returns:
#         dictionary of all jobs that match
#     """
#     w = WorkspaceClient()

#     all_workspace_jobs = w.jobs.list(expand_tasks=False)
#     matching_jobs_index = {}

#     # iterate through each job in the workspace to inspect the tags to see if it 
#     # matches the specified tag_name=tag_value
#     for job in all_workspace_jobs:
#         # not all jobs will have tags - these will be ignored as it is a standards requirement to tag jobs
#         if job.settings.tags:
#             if tag_name in job.settings.tags:
#                 if re.fullmatch(tag_value_regexp, job.settings.tags[tag_name]):
#                     # store the matching job
#                     matching_jobs_index[job.job_id] = w.jobs.get(job.job_id).as_dict()['settings']

#     return matching_jobs_index


# def _extract_delta_live_tables() -> dict:
#     """Extract a dictionary of pipeline_id={definition} key value pairs for all Delta Live tables in the system

#     Returns:
#         dictionary of all Delta Live Tables
#     """
#     w = WorkspaceClient()
#     pipeline_index = {}

#     # iterate through each pipeline and return the specification for it
#     for pipeline in w.pipelines.list_pipelines():
#         pipeline_detail_dict = w.pipelines.get(pipeline.pipeline_id).as_dict()
#         pipeline_index[pipeline.pipeline_id] = pipeline_detail_dict['spec']

#     return pipeline_index

# def _prepare_output_directories(list_dirs:list, clear_existing:bool, file_extension:str) -> None:
#     """prepare file directories to recieve the job and dlt outputs

#     Creates the directory if it doesn't exist
#     If it does, optionally deletes out any existing files of the expected file extension

#     Args:
#         list_dirs - list of directories to be prepared
#         clear_existing - if True, any existing files matching the extension are deleted
#         file_extension - file extention of files to be deleted

#     """
#     for path in list_dirs:
#         if not os.path.exists(path):
#             os.makedirs(path)
#         else:
#             if clear_existing:
#                 for existing_file in glob.glob(path + '/*' + file_extension):
#                     os.remove(existing_file)

# def _extract_variable_name(output_format:str, new_value:str, existing_value:str) -> (str, str):
#     """Extract the name of any variable contained in the value string.

#     Args:
#         output_format - value "json" or "yaml" indicating in which format the variable is encoded
#         new_value - string being scanned for variables
#         existing_value - full existing value to extract the desired current value

#     Returns:
#         string containing the first variable name (minus tags) in the string
#         or None if the string contains no variables
#     """
#     if output_format=='yaml':
#         token_start = '${var.'
#         token_end = '}'
#     else:
#         token_start = '^[^'
#         token_end = '^]^'

#     # does the introducer token exist in the string
#     start_position = new_value.find(token_start)
#     if start_position >= 0:
#         # check for a token end and extract the value of the token
#         candidate_token = new_value[(start_position+len(token_start)):]
#         end_position = candidate_token.find(token_end)
#         if end_position:
#             if type(existing_value) is str:
#                 # for a string variable it is possible that the variable features within the string 
#                 return candidate_token[:end_position], existing_value.replace(candidate_token[end_position+len(token_end):], '').replace(new_value[:start_position], '')
#             else:
#                 # for non-string values just return the value unmodified
#                 return candidate_token[:end_position], existing_value

#     # nothing found - or a token_start but no end
#     return None, None

# def _extract_variables(output_format:str, substitution_logs:list) -> dict:
#     """Produce a template variable file
    
#     Produces a template variable file from the list of substitutions.

#     Args:
#         output_format - string indicating of the format should be for json or yaml
#         substitution_logs - list of variable substitutions recorded for all jobs and DLTs
    
#     """

#     found_variables = {}

#     # iterate through each substitution
#     for job_name, path, existing_value, new_value in substitution_logs:
#         # where a value is a string then look for variable names in it
#         if type(new_value) is str:
#             found_variable, updated_existing_value = _extract_variable_name(output_format, new_value, existing_value)
#             if found_variable:
#                 # if not seen before add a new entry otherwise ignore
#                 if found_variable not in found_variables:
#                     found_variables[found_variable] = updated_existing_value
#         elif type(new_value) is list:
#             # scan each list item
#             for list_index in range(len(new_value)):
#                 if len(existing_value) < list_index:
#                     candidate_existing_value = existing_value[list_index]
#                 else:
#                     candidate_existing_value = ''
                    
#                 found_variable, actual_existing_value = _extract_variable_name(output_format, new_value[list_index], candidate_existing_value)
#                 if found_variable:
#                     if found_variable not in found_variables:
#                         # add the corresponding value from the list of existing values
#                         found_variables[found_variable] = actual_existing_value

#     return found_variables

# def _write_variables(param_output_path:str, dict_variables:dict, output_format:str) -> None:
#     """write out a sample variable file

#     Creates a sample variable file containing one entry for each variable inserted into the job and DLT
#     definitions using the value from the first occurance in the source environment.  The format
#     is always a yaml file but for json this will be a cicd variable format file whereas for yaml this will
#     be an asset bundle format

#     Args:
#         param_output_path - path to write the sample variable file
#         dict_variables - key:value pairs containing the variable name and sample value
#         output_format - string either json or yaml 
#     """

#     if output_format=='yaml':
#         file_name = 'target_sample.yml'
#         dict_output = {}
#         dict_master = { 'variables': dict_output }
#     else:
#         file_name = 'sample-var.yml'
#         lst_output = [ { 'name': 'hulahoop', 'value': 123 }]
#         dict_master = { 'variables': lst_output }
    
#     # add each variable in the relevent format
#     for variable_name, variable_value in dict_variables.items():
#         # for yaml, it's a dictionary entry with the variable name 
#         if output_format=='yaml':
#             dict_output[variable_name] = {
#                 'description': f'Description for variable {variable_name}',
#                 'default': variable_value
#             }
#         else:
#         # for json, it's a list of dictionaries
#             lst_output.append({
#                 'name': variable_name,
#                 'value': variable_value
#             })

#     with open(param_output_path + '/' + file_name, 'w') as f_output:
#         # output is always yaml
#         yaml.dump(dict_master, f_output, default_style='') #, default_flow_style=False)


# def export_jobs_pipelines(control_file:str, job_output_path:str, dlt_output_path:str, param_output_path:str=None, clear_existing:bool=False,
#                           object_renamer=lambda x:x) -> None:
#     # read in the control file which contains the information on which jobs to extract and what format to write the output in
#     with open(control_file) as f_json:
#         control_dict = json.load(f_json)
    
#     # read the output format and calculate the extension to be used by the target files
#     output_format = control_dict.get('output_format', '(not specified)')
#     if output_format == 'yaml':
#         file_extension = '.yml'
#     elif output_format == 'json':
#         file_extension = '.json'
#     else:
#         raise ValueError(f'Output format {output_format} is not valid')

#     # if set to True, use the new group name format e.g. DEV-GROUP_NAME-OPERATOR.  If not present, or False,
#     # use Dev - Group_Name - Operator
#     if 'job_permissions' in control_dict:
#         job_permissions = control_dict['job_permissions']
#     elif control_dict.get('new_group_name_format', False):
#         job_permissions = [
#             { 'group_name': '${var.account_group_environment_prefix_ucase}-${var.account_group_dataset_name}-OPERATOR', 'level': 'CAN_MANAGE'},
#             { 'group_name': '${var.account_group_environment_prefix_ucase}-${var.account_group_dataset_name}-SUPPORT', 'level': 'CAN_VIEW'},
#         ]
#     else:
#         job_permissions = [
#             { 'group_name': '${var.account_group_environment_prefix} - ${var.account_group_dataset_name} - Operator', 'level': 'CAN_MANAGE'},
#             { 'group_name': '${var.account_group_environment_prefix} - ${var.account_group_dataset_name} - Support', 'level': 'CAN_VIEW'},
#         ]

#     # get a dictionary of jobs to be migrated where the key is the local job id and the value is the job name
#     migrate_jobs_dict = _extract_candidate_job_list(tag_name=control_dict['job_extract']['tag_name'],
#                                                                 tag_value_regexp=control_dict['job_extract']['tag_value'])

#     if not migrate_jobs_dict:
#         raise ValueError(f'No matching jobs found to extract')

#     # list to record each variable substitution made in each file
#     lst_substitutions = []

#     # return a complete list of DLTs.  As the jobs are exported a list of referenced DLTs is created and then used for export
#     all_dlt_dict = _extract_delta_live_tables()
#     dlts_to_export = []

#     list_dirs=[job_output_path, dlt_output_path]
#     if param_output_path:
#         list_dirs.append(param_output_path)

#     _prepare_output_directories(list_dirs, clear_existing=clear_existing, file_extension=file_extension)

#     for job_id, job_settings in migrate_jobs_dict.items():
#         # extract the job name and rename it
#         job_name = object_renamer(job_settings['name'])

#         # before applying the generic paramiterisation, apply special handling for Delta Live Table tasks and run job tasks
#         for task in job_settings.get('tasks', []):
#             # handling for run DLT task types
#             if 'pipeline_task' in task:
#                 # extract the Id of the pipeline
#                 run_pipeline_id = task['pipeline_task']['pipeline_id']

#                 # if this is not a valid pipeline Id, abort now.  This can happen if the DLT is deleted after the job task is created
#                 if run_pipeline_id not in all_dlt_dict:
#                     raise ValueError(f'Exporting job {job_name} failed as it references an unknown DLT Id {run_pipeline_id}')

#                 # record the pipeline id for export later
#                 if run_pipeline_id not in dlts_to_export:
#                     dlts_to_export.append(run_pipeline_id)

#                 # use the name of the pipeline for lookup in higher environments
#                 pipeline_name = object_renamer(all_dlt_dict[run_pipeline_id]['name'])

#                 # substitue the pipeline id for an apporpriate parameter
#                 if output_format=='yaml':
#                     task['pipeline_task']['pipeline_id'] = '${resources.pipelines.' + pipeline_name+ '.id}'
#                 else:
#                     task['pipeline_task']['pipeline_id'] = '^[^' + pipeline_name + '^]^'
#             # handling for run job task types
#             elif 'run_job_task' in task:
#                 # get the id of the job that this task runs
#                 run_job_id = task['run_job_task']['job_id']

#                 # if the job being run is not in the list of jobs being exported then exit now
#                 if run_job_id not in migrate_jobs_dict:
#                     raise ValueError(f'Exporting job {job_name} failed as it references a job_id not being exported {run_job_id}')

#                 # name of the job being run to use in the substitution
#                 run_job_name = object_renamer(migrate_jobs_dict[run_job_id]['name'])

#                 # note that this functionality isn't yet supported by DAB
#                 if output_format=='yaml':
#                     task['run_job_task']['job_id'] = '${resources.jobs.' + run_job_name + '.id}'
#                 else:
#                     task['run_job_task']['job_id'] = '^[^' + run_job_name + '^]^'

#         environment_override = {}

#         # now make variable substitutions as specified in the control file
#         _parameterise_job_definition(job_settings, control_dict.get('job_parameterisation', {}), lst_substitutions, output_format, environment_override)

#         # write the updated name before it gets written out
#         job_settings['name'] = job_name

#         with open(job_output_path + '/' + job_name + file_extension, 'w') as f_output:
#             # additional processing for yaml files
#             if output_format=='yaml':
#                 # add standard permission lines
#                 job_settings['permissions'] = job_permissions
 
#                 # reformat the directory into the standard for an asset bundle
#                 asset_bundle_dict = {
#                     'resources': {
#                         'jobs': {
#                             job_name: job_settings
#                         }
#                     }
#                 }

#                 # are there any overrides?
#                 if environment_override:
#                     # create a dictionary to store the environment specific targets
#                     asset_bundle_dict['targets'] = {}

#                     # process each environment
#                     for env_code, overrides in environment_override.items():
#                         asset_bundle_dict['targets'][env_code] = { 'resources': { 'jobs': { job_name: overrides}}}

#                 # write out the yaml
#                 yaml.dump(asset_bundle_dict, f_output, width=999) #, default_flow_style=False)
#             else:
#                 json.dump(job_settings, f_output, indent=4, sort_keys=True)

#     # next export the DLTs referenced in the exported jobs
#     for dlt_id in dlts_to_export:
#         dlt_settings = all_dlt_dict[dlt_id]

#         # extract the name and apply any appropriate renaming
#         dlt_name = object_renamer(dlt_settings['name'])
#         environment_override = {}

#         _parameterise_job_definition(dlt_settings,control_dict.get('dlt_parameterisation', {}), lst_substitutions, output_format, environment_override)

#         dlt_settings['name'] = dlt_name

#         with open(dlt_output_path + '/' + dlt_name + file_extension, 'w') as f_output:
#             if output_format=='yaml':
#                 if 'dlt_permissions' in control_dict:
#                     job_permissions = control_dict['job_permissions']
#                 elif control_dict.get('new_group_name_format', False):
#                     dlt_settings['permissions'] = [
#                         { 'group_name': '${var.account_group_environment_prefix_ucase}-${var.account_group_dataset_name}-OPERATOR', 'level': 'CAN_MANAGE'},
#                         { 'group_name': '${var.account_group_environment_prefix_ucase}-${var.account_group_dataset_name}-SUPPORT', 'level': 'CAN_VIEW'},
#                     ]
#                 else:
#                     # add standard permission lines
#                     dlt_settings['permissions'] = [
#                         { 'group_name': '${var.account_group_environment_prefix} - ${var.account_group_dataset_name} - Operator', 'level': 'CAN_MANAGE'},
#                         { 'group_name': '${var.account_group_environment_prefix} - ${var.account_group_dataset_name} - Support', 'level': 'CAN_VIEW'},
#                     ]

#                 # reformat the directory into the standard for an asset bundle
#                 asset_bundle_dict = {
#                     'resources': {
#                         'pipelines': {
#                             dlt_name: dlt_settings
#                         }
#                     }                 
#                 }
#                 # are there any overrides?
#                 if environment_override:
#                     # create a dictionary to store the environment specific targets
#                     asset_bundle_dict['targets'] = {}

#                     # process each environment
#                     for env_code, overrides in environment_override.items():
#                         asset_bundle_dict['targets'][env_code] = { 'resources': { 'pipelines': { dlt_name: overrides}}}


#                 yaml.dump(asset_bundle_dict, f_output, width=999) #, default_flow_style=False)                
#             else:
#                 json.dump(dlt_settings, f_output, indent=4, sort_keys=True)

#     # is there a parameter path to write an example parameter file to
#     if param_output_path:
#         dict_variables = _extract_variables(output_format, lst_substitutions)
#         _write_variables(param_output_path, dict_variables, output_format)


# COMMAND ----------

# # job_extract1

# from pathlib import Path
# from job_extract import export_jobs_pipelines  # make sure this function exists

# # Log start
# print("✅ job_extract1 started")

# # Get parameters from the parent notebook
# control_file = dbutils.widgets.get("control_file")
# job_output_path = dbutils.widgets.get("job_output_path")
# dlt_output_path = dbutils.widgets.get("dlt_output_path")
# clear_existing = dbutils.widgets.get("clear_existing") == "True"

# print(f"Parameters received:\n"
#       f"  control_file: {control_file}\n"
#       f"  job_output_path: {job_output_path}\n"
#       f"  dlt_output_path: {dlt_output_path}\n"
#       f"  clear_existing: {clear_existing}")

# # Run the export
# result = export_jobs_pipelines(
#     control_file=control_file,
#     job_output_path=job_output_path,
#     dlt_output_path=dlt_output_path,
#     clear_existing=clear_existing
# )

# print("✅ Exporting pipelines complete!")

# # Return result to calling notebook
# dbutils.notebook.exit(str(result))


# COMMAND ----------

import os
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import explode_outer,col, when, to_timestamp, date_format, to_date, current_timestamp, lit,substring_index, instr,current_user,concat,row_number
from pyspark.sql.types import StringType, TimestampType,StructType, ArrayType,DateType
from pathlib import Path
import pandas as pd 
from functools import reduce
from pyspark.sql.window import Window

# COMMAND ----------

# job_extract1 notebook (paste this whole cell)
from pathlib import Path
import traceback

# IMPORTANT: import from the python file we just created
# from job_extract.job_extract1 import export_jobs_pipelines

import sys
sys.path.append("/Workspace/Repos/madhu.sudhanraochitty@gmail.com/my_CICD_project/job_extract/job_extract1")

# from job_extract1 import export_jobs_pipelines
dbutils.import_notebook("job_extract1") 


print("✅ job_extract1 started")

try:
    # read parameters from widgets if called by the parent notebook
    try:
        control_file = dbutils.widgets.get("control_file")
        job_output_path = dbutils.widgets.get("job_output_path")
        dlt_output_path = dbutils.widgets.get("dlt_output_path")
        param_output_path = dbutils.widgets.get("param_output_path") if "param_output_path" in dbutils.widgets.getArgumentNames() else None
        clear_existing = dbutils.widgets.get("clear_existing") == "True"
    except Exception:
        # Standalone defaults for local testing
        print("⚠️ Running standalone — using default test paths.")
        current_notebook_path = Path.cwd()
        repo_root_path = current_notebook_path.parent
        control_file = str(current_notebook_path) + '/olds_jobs_dlts.json'
        job_output_path = str(repo_root_path) + '/bundle/jobs/'
        dlt_output_path = '/tmp'
        param_output_path = None
        clear_existing = True

    print("Parameters received:")
    print("  control_file:", control_file)
    print("  job_output_path:", job_output_path)
    print("  dlt_output_path:", dlt_output_path)
    print("  param_output_path:", param_output_path)
    print("  clear_existing:", clear_existing)

    # run the exporter
    result = export_jobs_pipelines(
        control_file=control_file,
        job_output_path=job_output_path,
        dlt_output_path=dlt_output_path,
        param_output_path=param_output_path,
        clear_existing=clear_existing
    )

    print("✅ export_jobs_pipelines finished successfully")
    dbutils.notebook.exit(str(result))

except Exception as e:
    print("❌ ERROR in job_extract1 notebook:")
    print(traceback.format_exc())
    dbutils.notebook.exit(f"FAILED: {e}")


# COMMAND ----------

from job_extract.exporter import export_jobs_pipelines

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/madhu.sudhanraochitty@gmail.com/my_CICD_project")


# COMMAND ----------

from pathlib import Path
import traceback
import sys

# Add your repo path to sys.path for imports
sys.path.append("/Workspace/Repos/madhu.sudhanraochitty@gmail.com/my_CICD_project/job_extract/job_extract")

# Import the function directly
from job_extract import export_jobs_pipelines

print("✅ job_extract1 started")

try:
    # Read parameters from widgets if called by the parent notebook
    try:
        control_file = dbutils.widgets.get("control_file")
        job_output_path = dbutils.widgets.get("job_output_path")
        dlt_output_path = dbutils.widgets.get("dlt_output_path")
        param_output_path = (
            dbutils.widgets.get("param_output_path")
            if "param_output_path" in dbutils.widgets.getArgumentNames()
            else None
        )
        clear_existing = dbutils.widgets.get("clear_existing") == "True"
    except Exception:
        # Standalone defaults for local testing
        print("⚠️ Running standalone — using default test paths.")
        current_notebook_path = Path.cwd()
        repo_root_path = current_notebook_path.parent
        control_file = str(current_notebook_path) + '/olds_jobs_dlts.json'
        job_output_path = str(repo_root_path) + '/bundle/jobs/'
        dlt_output_path = '/tmp'
        param_output_path = None
        clear_existing = True

    print("Parameters received:")
    print("  control_file:", control_file)
    print("  job_output_path:", job_output_path)
    print("  dlt_output_path:", dlt_output_path)
    print("  param_output_path:", param_output_path)
    print("  clear_existing:", clear_existing)

    # Run the exporter
    result = export_jobs_pipelines(
        control_file=control_file,
        job_output_path=job_output_path,
        dlt_output_path=dlt_output_path,
        param_output_path=param_output_path,
        clear_existing=clear_existing
    )

    print("✅ export_jobs_pipelines finished successfully")
    dbutils.notebook.exit(str(result))

except Exception as e:
    print("❌ ERROR in job_extract1 notebook:")
    print(traceback.format_exc())
    dbutils.notebook.exit(f"FAILED: {e}")
