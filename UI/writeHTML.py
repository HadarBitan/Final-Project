import csv

# Read data from CSV file
csv_file = 'data.csv'
data = []
with open(csv_file, 'r') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Read the header row
    for row in reader:
        data.append(row)

# Count the occurrences of "Is Malicious" and "Not Malicious"
is_malicious_count = 0
not_malicious_count = 0
for row in data:
    if row[2] == 'True':
        is_malicious_count += 1
    else:
        not_malicious_count += 1

# Calculate the percentage of "Is Malicious" and "Not Malicious"
total_count = len(data)
is_malicious_percentage = (is_malicious_count / total_count) * 100
not_malicious_percentage = (not_malicious_count / total_count) * 100

# Generate HTML markup
html = '''
<html>
<head>
  <title>Paypal Malicious Activity</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background-color: #F0F0F0;
      margin: 0;
      padding: 20px;
    }}
    h1 {{
      text-align: center;
      color: #333333;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      background-color: #FFFFFF;
      border: 1px solid #CCCCCC;
      box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);
    }}
    th, td {{
      text-align: left;
      padding: 8px;
    }}
    th {{
      background-color: #333333;
      color: #FFFFFF;
    }}
    tr:nth-child(even) {{
      background-color: #FAFAFA;
    }}
    tr.null {{
      background-color: #CCD9E8;
    }}
    tr.true {{
      background-color: #FFCCCC;
    }}
    .chart-container {{
      width: 500px;
      margin: 20px auto;
    }}
    .chart-bar {{
      display: inline-block;
      height: 20px;
      background-color: #CCCCCC;
      margin-bottom: 10px;
    }}
    .chart-bar.is-malicious {{
      background-color: #FF6666;
    }}
    .chart-bar.not-malicious {{
      background-color: #66CC66;
    }}
    .image-container {{
      position: absolute;
      top: 20px;
      left: 20px;
    }}
  </style>
</head>
<body>
  <div class="image-container">
    <img src="paypalPic.png"  width="100" height="50">
  </div>
  <h1>Paypal Malicious Activity</h1>
  <table>
    <tr>
      {}
    </tr>
    {}
  </table>
  <div class="chart-container">
  <p>Is Malicious: {}%  <div class="chart-bar is-malicious" style="width: {}%;"></div> </p>
  <p>Not Malicious: {}%  <div class="chart-bar not-malicious" style="width: {}%;"></div></p>
    
    
  </div>
  
  
</body>
</html>
'''.format(
    ''.join(f'<th style="background-color: #333333; color: #FFFFFF;">{column}</th>' for column in header),
    ''.join(f'<tr class="null">{"".join(f"<td>{value}</td>" for value in row)}</tr>' if row[2] is None or row[2] != 'True' else f'<tr class="true">{"".join(f"<td>{value}</td>" for value in row)}</tr>' for row in data),
    is_malicious_percentage,
    is_malicious_percentage,
    not_malicious_percentage,
    not_malicious_percentage
)

# Write the HTML to a file
html_file = 'data.html'
with open(html_file, 'w') as f:
    f.write(html)

print(f"HTML file '{html_file}' created successfully.")
