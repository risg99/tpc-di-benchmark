import os
import numpy as np
import pandas as pd
import xmltodict
import json

def customermgmt_convert():
    with open('dags/sf_3/Batch1/CustomerMgmt.xml') as fd:
        doc = xmltodict.parse(fd.read()) 
        fd.close()

    with open("dags/sf_3/Batch1/CustomerData.json", "w") as outfile:
        outfile.write(json.dumps(doc))
        outfile.close()

    f = open('dags/sf_3/Batch1/CustomerData.json','r')

    cust = json.load(f)
    actions = cust['TPCDI:Actions']
    action = actions['TPCDI:Action']
    cust_df = pd.DataFrame(columns = np.arange(0, 36))


    for a in action:
        
        cust_row = {}
        
        # action element
        cust_row.update({0: [f"{a.get('@ActionType')}"]})
        cust_row.update({1: [f"{a.get('@ActionTS')}"]})
        
        # action.customer element
        cust_row.update({2: [f"{a.get('Customer').get('@C_ID')}"]})
        cust_row.update({3: [f"{a.get('Customer').get('@C_TAX_ID')}"]})
        cust_row.update({4: [f"{a.get('Customer').get('@C_GNDR')}"]})
        cust_row.update({5: [f"{a.get('Customer').get('@C_TIER')}"]})
        cust_row.update({6: [f"{a.get('Customer').get('@C_DOB')}"]})
        
        # action.customer.name element
        if a.get('Customer').get('Name') != None:
            cust_row.update({7: [f"{a.get('Customer').get('Name').get('C_L_NAME')}"]})
            cust_row.update({8: [f"{a.get('Customer').get('Name').get('C_F_NAME')}"]})
            cust_row.update({9: [f"{a.get('Customer').get('Name').get('C_M_NAME')}"]})
        else:
            cust_row.update({7: [None]})
            cust_row.update({8: [None]})
            cust_row.update({9: [None]})
        
        # action.customer.address element
        if a.get('Customer').get('Address') != None:
            cust_row.update({10: [f"{a.get('Customer').get('Address').get('C_ADLINE1')}"]})
            cust_row.update({11: [f"{a.get('Customer').get('Address').get('C_ADLINE2')}"]})
            cust_row.update({12: [f"{a.get('Customer').get('Address').get('C_ZIPCODE')}"]})
            cust_row.update({13: [f"{a.get('Customer').get('Address').get('C_CITY')}"]})
            cust_row.update({14: [f"{a.get('Customer').get('Address').get('C_STATE_PROV')}"]})
            cust_row.update({15: [f"{a.get('Customer').get('Address').get('C_CTRY')}"]})
        else:
            cust_row.update({10: [None]})
            cust_row.update({11: [None]})
            cust_row.update({12: [None]})
            cust_row.update({13: [None]})
            cust_row.update({14: [None]})
            cust_row.update({15: [None]})
            
        # action.customer.contactinfo element
        if a.get('Customer').get('ContactInfo') != None:     
            cust_row.update({16: [f"{a.get('Customer').get('ContactInfo').get('C_PRIM_EMAIL')}"]})
            cust_row.update({17: [f"{a.get('Customer').get('ContactInfo').get('C_ALT_EMAIL')}"]})
            
            # action.customer.contactinfo.phone element
            
            # phone_1
            cust_row.update({18: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_CTRY_CODE')}"]})
            cust_row.update({19: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_AREA_CODE')}"]})
            cust_row.update({20: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_LOCAL')}"]})
            cust_row.update({21: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_EXT')}"]})

            # phone_2
            cust_row.update({22: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_CTRY_CODE')}"]})
            cust_row.update({23: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_AREA_CODE')}"]})
            cust_row.update({24: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_LOCAL')}"]})
            cust_row.update({25: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_EXT')}"]})
        
            # phone_3
            cust_row.update({26: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_CTRY_CODE')}"]})
            cust_row.update({27: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_AREA_CODE')}"]})
            cust_row.update({28: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_LOCAL')}"]})
            cust_row.update({29: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_EXT')}"]})
        else:
            cust_row.update({16: [None]})
            cust_row.update({17: [None]})
            cust_row.update({18: [None]})
            cust_row.update({19: [None]})
            cust_row.update({20: [None]})
            cust_row.update({21: [None]})
            cust_row.update({22: [None]})
            cust_row.update({23: [None]})
            cust_row.update({24: [None]})
            cust_row.update({25: [None]})
            cust_row.update({26: [None]})
            cust_row.update({27: [None]})
            cust_row.update({28: [None]})
            cust_row.update({29: [None]})
        
        # action.customer.taxinfo element
        if a.get('Customer').get('TaxInfo') != None:
            cust_row.update({30: [f"{a.get('Customer').get('TaxInfo').get('C_LCL_TX_ID')}"]})
            cust_row.update({31: [f"{a.get('Customer').get('TaxInfo').get('C_NAT_TX_ID')}"]})
        else:
            cust_row.update({30:  [None]})
            cust_row.update({31:  [None]})
        
        # action.customer.account attribute
        if a.get('Customer').get('Account') != None:
            cust_row.update({32: [f"{a.get('Customer').get('Account').get('@CA_ID')}"]})
            cust_row.update({33: [f"{a.get('Customer').get('Account').get('@CA_TAX_ST')}"]})
            
            # action.customer.account element
            cust_row.update({34: [f"{a.get('Customer').get('Account').get('CA_B_ID')}"]})
            cust_row.update({35: [f"{a.get('Customer').get('Account').get('CA_NAME')}"]})
        else:
            cust_row.update({32: [None]})
            cust_row.update({33: [None]})
            cust_row.update({34: [None]})
            cust_row.update({35: [None]})
        
        # append to dataframe
        cust_df = pd.concat([cust_df, pd.DataFrame.from_dict(cust_row)], axis = 0)

    cust_df.replace(to_replace = np.NaN, value = "", inplace = True)
    cust_df.replace(to_replace = "None", value = "", inplace = True)
    cust_df.to_csv('dags/sf_3/Batch1/CustomerMgmt.csv', index = False)
    print('Customer Management data converted from XML to CSV')