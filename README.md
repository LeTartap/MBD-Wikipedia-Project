
#### **Objective:**
Analyze Wikipedia usage trends over the past 15 years, focusing on **new user accounts**, **article view counts**, and **number of edits**, while exploring the impact of generative AI, major world events, and potential anomalies in specific topics or categories.

---

### **1. Data Collection**

**Task 1.1**: **Extract Historical Data**
- Use the Wikimedia Analytics API to collect:
  - [] Number of **new user accounts** created per year (2008–2024).
  - [] **Article view counts** (daily/monthly aggregated) for relevant topics/categories.
  - [] **Number of edits** per year for all articles or specific topics.
- Deliverable: Cleaned dataset containing yearly trends.

**Task 1.2**: **Retrieve Revision History**
- Use the Wikimedia Action API to fetch:
  - [] Revision history for specific topics (e.g., US Elections, COVID-19, War in Ukraine).
  - [x] Revision IDs, timestamps, editors (new vs. old accounts), and size of changes.
- Deliverable: [] Dataset of revisions for selected articles.

**Task 1.3**: **Identify Categories**
- Explore the API for article categorization (e.g., portals, templates, or categories).
- Focus on meaningful categories like:
  - US Politics (e.g., elections, governance).
  - Global crises (e.g., COVID-19, Ukraine War).
  - Articles relevant to vulnerable demographics.

---

### **2. Data Analysis**

**Task 2.1**: **Analyze Long-Term Trends**
- Calculate trends and periodicity for:
  - New user accounts.
  - Article views.
  - Number of edits.
- Highlight major shifts over time.

**Task 2.2**: **Impact of Generative AI**
- Identify milestones in generative AI adoption (e.g., GPT-2 in 2019, GPT-3 in 2020, ChatGPT in 2022).
- Compare trends before and after each milestone:
  - Edits per user (baseline vs. AI era).
  - Number of edits by new accounts.
  - Frequency of specific keywords (e.g., "AI", "ChatGPT") in article edits.
- Deliverable: Visualizations of trends with AI-related milestones.

**Task 2.3**: **Impact of Major World Events**
- Select events such as:
  - COVID-19 (2020–2022).
  - War in Ukraine (2022–present).
  - US Elections (2016, 2020, 2024).
- Analyze:
  - Peaks in views and edits.
  - Increased activity by new accounts.
  - Content stability (e.g., frequency of large edits, rapid reversals).

**Task 2.4**: **Detect AI-Sourced Contributions**
- Develop indicators for generative AI use:
  - Sudden increases in edits by new accounts.
  - Overlap of common phrases from generative AI (e.g., repetitive language patterns).
  - Use external tools to detect AI-written text in revisions (e.g., GPT detectors).
- Deliverable: Report on AI-sourced contributions.

---

### **3. Topic-Specific Investigations**

**Task 3.1**: **Compare US Elections**
- Select key articles from past elections (2008, 2012, 2016, 2020, 2024).
- Analyze:
  - Edits per user.
  - Number of revisions before, during, and after each election.
  - Article size growth over time.

**Task 3.2**: **Focus on War in Ukraine**
- Track views and edits for key articles (e.g., "Ukraine War", "Russia-Ukraine Conflict").
- Compare:
  - Content activity during major events (e.g., invasions, peace talks).
  - Edit patterns from accounts with low activity history.

**Task 3.3**: **Monitor COVID-19 Articles**
- Analyze view and edit trends during key pandemic milestones:
  - Outbreak in early 2020.
  - Vaccine rollouts in 2021.
  - Decline in activity post-2022.
- Identify anomalies in user behavior (e.g., bulk edits).

---

### **4. Metrics and Models**

**Task 4.1**: **Edits Per User**
- Calculate the function of edits per user over time (2008–2024).
- Compare:
  - Pre-2020 baseline.
  - Post-2020 period (impact of generative AI and world events).

**Task 4.2**: **Text Analysis**
- Measure:
  - Average size of edits.
  - Common keywords (e.g., “election fraud”, “AI-generated”).
  - Sentiment analysis for controversial topics.
- Deliverable: Insights into user behavior and content shifts.

---

### **5. Visualization and Reporting**

**Task 5.1**: **Generate Visual Trends**
- Plot:
  - New accounts per year.
  - Views and edits for selected topics.
  - Peaks during generative AI milestones and world events.

**Task 5.2**: **Interactive Dashboard**
- Create a dashboard showing:
  - Editable graphs for views, edits, and accounts over time.
  - Filters for specific topics (e.g., elections, pandemics).
- Deliverable: Dashboard hosted locally or on a cloud platform.

**Task 5.3**: **Prepare Final Report**
- Summarize findings with:
  - Key trends.
  - Evidence of generative AI contributions.
  - Impact of world events on Wikipedia usage.
- Deliverable: Report with actionable insights and visualizations.

---

### **Stretch Goals**
- Predict future trends in views and edits using machine learning models.
- Build a tool to detect generative AI edits in real-time.

---

### **Next Steps**
- Finalize the specific topics and categories to analyze.
- Set up the data pipeline to fetch data from Wikimedia APIs.
- Begin with data exploration to validate hypotheses.

