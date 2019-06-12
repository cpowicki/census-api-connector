import requests
import pandas as pd
import json

class ACS_connection:

    def __init__(self, api_key=None, table="detail"):
        self.key = api_key
        if self.key == None:
            print("Warning - Initialized without API key. The number of API requests that may be sent by this IP Address is capped" +\
                " and enforced by census.gov. Obtain API key to send unlimitted requests.")
        self.table = table
        self.valid_years = [str(i) for i in range(2009, 2018)]
        self.valid_tables = ["detail", "profile"]
        self.supported_geo = ["us", "tract", "county", "state"]

    def change_table(self, table):
        self.table = table
    
    def query(self, year, fields, geography={"us":"*"}, output="json"):
        if str(year) not in self.valid_years:
            raise ACSException("Invalid Year: " + str(year))

        endpoint =  "https://api.census.gov/data/" + str(year)
        
        if year == "2009":
            endpoint += "/acs5"
        else:
            endpoint += "/acs/acs5"
        
        if self.table == "profile":
            if year == "2009" or year == "2010":
                raise ACSException("No API Access for Profile Table for " + year)
            endpoint += "/profile"
        
        endpoint += "?get=NAME," + ','.join(fields)
        sendpoints = []
        if "us" in geography.keys():
            endpoint += "&for=us:*"
            sendpoints.append(endpoint)
        else:
            if "tract" in geography.keys():
                try:
                    if geography["state"] == ["*"]:
                        raise ACSException("Specified state(s) required for Census Tract query.")
                except KeyError:
                    raise ACSException("Specified state(s) required for Census Tract query.")
                
                if geography["tract"] == ["*"]:
                    if len(geography["state"]) > 1:
                        for i in geography["state"]:
                            sendpoints.append(endpoint + "&for=tract:*&in=state:" + i)
                    else:
                        endpoint += "&for=tract:*&in=state:"+ geography["state"][0]
                        sendpoints.append(endpoint)
                else:
                    pass
            elif "county" in geography.keys():
                if geography["county"] == ["*"]:
                    if "state" not in geography.keys():
                        endpoint += '&for=county:*'
                    else:
                        endpoint += '&for=county:*&in=state:' + ','.join(geography["state"])
                else:
                    if "state" not in geography.keys():
                        raise ACSException("State required for non-wild County query.")
                    else:
                        endpoint += '&for=county:' + ','.join(geography["county"]) + '&in=state:' + ','.join(geography["state"])
                sendpoints.append(endpoint)
            
            elif "state" in geography.keys():
                endpoint += '&for=state:' + ','.join(geography["state"])
                sendpoints.append(endpoint)

        
        
        dfs = []
        for req in sendpoints:
            if self.key != None:
                req += "&key=" + self.key
            response = requests.get(req)
            if "error" in response.text:
                raise ACSException(response.text)
            obj = json.loads(response.text)
            df = pd.DataFrame(columns=obj[0], data=obj[1:])
            dfs.append(df)
        
        df_final = pd.DataFrame(columns=dfs[0].columns).append(dfs)

        if output == "json":
            return df_final.to_json(orient="records")
        elif output == "dataframe":
            return df_final
        elif output == "csv":
            df_final.to_csv("data.csv", index = False)
            return True
        elif output == "excel":
            df_final.to_excel("data.xlsx", index = False)
            return True
        
        return False
    

        
    
class ACSException(Exception):
    pass

# Main 
connection = ACS_connection(table="profile")

for y in connection.valid_years[2:]:
    print(connection.query(y, ["DP02_0001PE"], geography={
        "tract" : ["*"],
        "state" : ["22"]
    }, output="dataframe").head())
    
