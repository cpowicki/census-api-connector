import requests
import pandas as pd
import json

class ACS_connection:

    def __init__(self, api_key=None, table="detail"):
        self.key = api_key
        self.table = table
        self.valid_years = [str(i) for i in range(2009, 2018)]
        self.valid_tables = ["detail", "profile"]
        self.supported_geo = ["us", "tract", "county", "state"]

    def change_table(self, table):
        self.table = table
    
    def query(self, year, fields, geography={"us":"*"}, format="json"):
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

        if "us" in geography.keys():
            endpoint += "&for=us:*"
        else:
            # if "full_tract" in geography.keys():
            #     state = []
            #     county =
            if "tract" in geography.keys():
                try:
                    if geography["state"] == ["*"]:
                        raise ACSException("Specified state(s) required for Census Tract query.")
                except KeyError:
                    raise ACSException("Specified state(s) required for Census Tract query.")
                
                if geography["tract"] == ["*"]:
                    endpoint += "&for=tract:*&in=state:"+ ','.join(geography["state"])
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

        if self.key != None:
            endpoint += "&key=" + self.key

        response = requests.get(endpoint)
        if "error" in response.text:
            raise ACSException(response.text)

        obj = json.loads(response.text)
        df = pd.DataFrame(columns=obj[0], data=obj[1:])
        
        if format == "json":
            return df.to_json(orient="records")
        elif format == "dataframe":
            return df
        elif format == "csv":
            df.to_csv("data.csv", index = False)
            return True
        elif format == "excel":
            df.to_excel("data.xlsx", index = False)
            return True
        
        return False
    

        
    
class ACSException(Exception):
    pass

# Main 
connection = ACS_connection()

# print(connection.get_data("2009", ["B00001_001E"], geography={
#     "county" : ["*"],
#     "state" : ["22" , "01"]
# }, format="dataframe").head())

for y in connection.valid_years:
    print(connection.query(y, ["B00001_001E"], geography={
        "tract" : ["*"],
        "state" : ["22", "01"]
    }, format="dataframe").head())
    
