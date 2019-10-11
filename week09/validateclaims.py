import re
import sys


def main(argv):
      
    return
   
def validate(tuples):
        
    npi_cnt = 0
    last_name_cnt =0
    first_name_cnt=0
    gender_cnt =0
    entity_code_cnt=0
    state_cnt =0
    prov_type_cnt =0
    hcpcs_code_cnt =0
    
    error_cnt = 0
               
    if validate_npi(tuples[0]) != True:
        npi_cnt += 1
        error_cnt += 1
        
    if validate_prov_last_name(tuples[1]) != True:
        last_name_cnt += 1
        error_cnt += 1
        
    if tuples[4] == "I":
        if validate_prov_first_name(tuples[2]) != True:
            first_name_cnt += 1
            error_cnt += 1
            
        if validate_gender(tuples[3]) != True:
            gender_cnt += 1
            error_cnt += 1
 
    if validate_entity_code(tuples[4]) != True:
        entity_code_cnt += 1
        error_cnt += 1       
        
    if validate_state(tuples[5]) != True:
        state_cnt += 1
        error_cnt += 1    
            
    if validate_prov_type(tuples[6]) != True:
        prov_type_cnt += 1
        error_cnt += 1
        
    if validate_hcpcs_code(tuples[7]) != True:
        hcpcs_code_cnt +=1
        error_cnt +=1
              
    error_list = [npi_cnt,last_name_cnt,first_name_cnt,gender_cnt,entity_code_cnt, state_cnt, prov_type_cnt,hcpcs_code_cnt]
    
    if error_cnt > 0:
#       print('Errors', error_list)
        return False
    else:
        return True     
        
def validate_npi(token):  
    if len(token) == 0: 
            return False
    elif re.match("^\\d+$",token):
        return True
    else:
        return False
       
def validate_prov_last_name(token):
    if re.match("^\\D+$",token):
        return True
    else:
        return False
    
def validate_prov_first_name(token):
    if re.match("^\\D+$",token):
        return True
    else:
        return False
    
def validate_gender(token):
    gender = token.upper()
    if gender != "M" and gender != "F":
        return False
    else:
        return True
             
def validate_entity_code(token): 
    if token != "I" and token != "O":
        return False
    else :
        return True
        
                      
def validate_state(token):
    statename = token.upper()

    states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 
              'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 
              'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 
              'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 
              'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 
              'WV', 'WY']
    
    if statename in states:
        return True 
    else:
        return False

def validate_prov_type(token):
    if re.match("^[a-zA-Z-_ ]+$",token):
        return True
    else:
        return False    
    
def validate_hcpcs_code(token):   
    if re.match("^[0-9a-zA-Z]+$",token): 
        return True
    else:
        return False    
    
def validate_avg_submitted_chrg_amt(token):
    if re.match("^\$?[0-9]+(\.([0-9]{2}))?$", token):
        return True
    else:
        return False
     
    
def parse(line):
    tokens = line.split('\t')    
    parsed_tuple = (tokens[0],tokens[1],tokens[2],tokens[5],tokens[6],tokens[11],tokens[13],tokens[16])
    return parsed_tuple
    
"""
This following code is the entry point when this module is called directly.
It calls the main method, which controls program flow.
We will not use this module this way, but will either:
     a) call validate directly to test a set of fields; or
     b) call individual functions as part of nose tests. 
"""
if __name__ == "__main__":
    
    main(sys.argv[1:])                    