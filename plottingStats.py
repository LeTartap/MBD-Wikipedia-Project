import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv("/user/s2539829/SHARED_MBD/rev_data_SenLen")

# Convert date to datetime format for better plotting
df['date'] = pd.to_datetime(df['date'])

# Sort values by date
df = df.sort_values(by='date')

# Convert columns to numeric types (if necessary)
df['avg(word_count)'] = pd.to_numeric(df['avg(word_count)'])
df['variance'] = pd.to_numeric(df['variance'])
df['relative_variance'] = pd.to_numeric(df['relative_variance'])

# Set a larger figure size for better visibility
plt.figure(figsize=(12, 6))

# Plot the average word count over time
sns.lineplot(data=df, x='date', y='avg(word_count)', label="Avg Sentence Length", color='blue')

# Plot the variance over time
sns.lineplot(data=df, x='date', y='variance', label="Variance of Sentence Length", color='red')

# Add labels and title
plt.xlabel("Date")
plt.ylabel("Value")
plt.title("Wikipedia Sentence Length Trends Over Time")
plt.legend()

# Show the plot
plt.show()
