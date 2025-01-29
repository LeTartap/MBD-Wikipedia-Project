import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv("part-00000-9e800985-84a1-4f8f-a729-e095b08ba235-c000.csv")

# Convert date to datetime format for better plotting
df['date'] = pd.to_datetime(df['date'])

# Filter the DataFrame to include only dates from 2020 onwards
df = df[df['date'] >= '2020-01-01']

# Sort values by date
df = df.sort_values(by='date')

# Convert columns to numeric types (if necessary)
df['avg(word_count)'] = pd.to_numeric(df['avg(word_count)'])
df['variance'] = pd.to_numeric(df['variance'])
df['relative_variance'] = pd.to_numeric(df['relative_variance'])

# Set a larger figure size for better visibility
plt.figure(figsize=(12, 6))

# Plot the average word count over time


# Plot the variance over time
# sns.lineplot(data=df, x='date', y='relative_variance', label="Variance of Sentence Length", color='red')
# Resample the data to bi-weekly frequency and calculate the mean
bi_weekly_avg = df.resample('2W', on='date').mean()

# Calculate the mean before November 20th, 2022
#plot a vertical line at the date November 20th, 2022
plt.axvline(x=pd.to_datetime('2022-11-20'), color='red', linestyle='-', label="Release of ChatGPT to the public")

mean_before_nov_2022 = df[df['date'] < '2022-11-20']['relative_variance'].mean()
#round down to 4 decimal places
mean_before_nov_2022 = round(mean_before_nov_2022, 2)
# Plot the mean before November 20th, 2022
plt.axhline(y=mean_before_nov_2022, xmin=0, xmax=(df[df['date'] < '2022-9-29']['date'].max() - df['date'].min()).days / (df['date'].max() - df['date'].min()).days, color='orange', linestyle='--', linewidth=2, label=f"Mean Before ChatGPT: {mean_before_nov_2022}")

# Calculate the mean from November 20th, 2022 onwards
mean_after_nov_2022 = df[df['date'] >= '2022-11-20']['relative_variance'].mean()
#round up to 4 decimal places
mean_after_nov_2022 = round(mean_after_nov_2022, 2)

# Plot the mean from November 20th, 2022 onwards
plt.axhline(y=mean_after_nov_2022, color='purple', linestyle='--', linewidth=2, label=f"Mean After ChatGPT: {mean_after_nov_2022}", xmin=(df[df['date'] >= '2022-9-29']['date'].min() - df['date'].min()).days / (df['date'].max() - df['date'].min()).days)

print(mean_before_nov_2022)
print(mean_after_nov_2022)



# Plot the bi-weekly average word count over time
# sns.lineplot(data=bi_weekly_avg, x='date', y='avg(word_count)', label="Bi-Weekly Average Word Count", color='blue')

#add a line for a rolling mean
df['rolling_mean'] = df['relative_variance'].rolling(window=30).mean()
sns.lineplot(data=df, x='date', y='rolling_mean', label="30-Day Rolling Mean of Realtive Variance", color='green')



# Add labels and title
plt.xlabel("Date")
plt.ylabel("Value")
plt.title("Wikipedia Sentence Length Trends Over Time")
plt.legend()

# Show the plot
plt.show()
