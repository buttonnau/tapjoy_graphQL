import requests
import json
import pandas as pd

headers = {
    #paste key below
    'Authorization': 'Basic 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567=',
    'Accept': 'application/json; */*',
}

response = requests.post('https://api.tapjoy.com/v1/oauth2/token', headers=headers)
r = response.json()
access_token = "Bearer " + r['access_token']
print("access_token ",access_token)

# Initialize endcursor, hasnextpage, and a counter
endcursor = None
hasnextpage = True

# Define the GraphQL query with pagination
def make_query(after_cursor=None):
    return """
{
    publisher{
        apps(first: 200, after:AFTER) {
            nodes {
            id
            mediationInsights(timeRange: { from:"2023-05-01T00:00:00Z", until:"2023-06-01T00:00:00Z"}){
                reports{
                    revenue
                    }
                timestamps
                }
            name
            sdkApiKey
            }
                
            pageInfo {
            endCursor
            hasNextPage
            }
        }
    }
}
""".replace(
        "AFTER", '"{}"'.format(endcursor)
    )

# Define the GraphQL endpoint URL
url = 'https://api.tapjoy.com/graphql'

# Set the request headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': access_token
}

out=[]

while hasnextpage!=False:
    query=make_query(endcursor)
    r = requests.post(url, headers=headers, json={'query': query})
    r = r.json()
    #append data to list
    out.extend(r['data']['publisher']['apps']['nodes']) 
    #update endcurser&hasnextpage
    endcursor=r['data']['publisher']['apps']['pageInfo']['endCursor']
    hasnextpage=r['data']['publisher']['apps']['pageInfo']['hasNextPage']
    print("endcursor,hasnextpage", endcursor,hasnextpage)

# Serializing json
json_object = json.dumps(out, indent=4)
 
# Writing to sample.json
with open("sample.json", "w") as outfile:
    outfile.write(json_object)

# read json
with open('sample.json', encoding='utf-8') as f:
    data = json.loads(f.read())

df = pd.json_normalize(data, 
                      record_path=["mediationInsights", "reports"], 
                      meta=[["mediationInsights", "timestamps"],"id", "name", "sdkApiKey"])
df["revenue"] = df["revenue"].explode()
df["revenue_in_USD"] = df["revenue"]/1000000
df = df.drop('revenue', axis=1)

# save to csv
df.to_csv('test.csv', index=False, encoding='utf-8')