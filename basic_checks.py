import pandas as pd
from prettytable import PrettyTable
import plotly_express as px

columns_of_interest = ["osr_distance", "bf_distance", "difference"]

# df = pd.read_csv("distance_data_local_0.csv")
# print(df.loc[df['link_node']==2])

# print(df['difference'].describe())
# df = pd.read_csv("dist_data_local_multi_mill.csv")
df = pd.read_csv("san_check.csv")
df = df.loc[df['osr_distance'] > 0]
df['difference'] = df['osr_distance'] - df['bf_distance']
print(df.describe()[columns_of_interest])
desc_df = df.describe()[columns_of_interest].round(3)

# Convert to PrettyTable

table = PrettyTable()
table.field_names = ["Statistic"] + columns_of_interest  # Add headers

# Add rows from describe()
for row in desc_df.index:
    table.add_row([row] + list(desc_df.loc[row]))

# Print the table
print(table)

# Create Bubble Chart
fig = px.scatter(df, x="bf_distance", y="osr_distance", size="difference", color="difference",
                 title="Chart of Distance Differences",
                 labels={"bf_distance": "Distance (Geodesic)", "osr_distance": "Distance (OpenRouteService)",
                         "difference": "Difference (OSR - Geordesic)"}, color_continuous_scale='Sunsetdark')

# fig.show()

# df = pd.read_csv("time_run.csv")
df = df.loc[df['osr_distance'] > 0]
df['difference'] = df['osr_distance'] - df['bf_distance']
print(df.nlargest(5, "difference")
)

# b767ee89-3947-40fb-b454-3c51cc0fd888, 51529a95-680b-49ea-bea8-aebb67d0d536, 122ab1fa-9512-438c-9ce2-61df16506944, 032f45b8-0bb5-4856-b3bb-ff9c15adf57e, af1b85d4-40ce-4d9f-a320-efa90d1f3a1c
# acb872ac-fd93-4413-8a28-525124d1f985,0d42b5a0-b866-45fb-b692-eece1539e321,47646697-03dd-4dff-8504-18c4e1b165b5,2d3cb6a8-47cf-4781-ba9a-f51140644e95,b5fdca93-7fc6-4059-91fe-f248cddd7cee
# ndf = pd.DataFrame.from_dict(distance_data, orient='index')
# print(ndf.head())
# ndf['link_node'] = 2
# ndf.loc[~ndf.index.isin(uuid_list), 'link_node'] = 0
# ndf['difference'] = ndf['osr_distance'] - ndf['bf_distance']